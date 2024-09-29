use partialzip::PartialZip;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::Document as TantivyDocument;
use tantivy::{Index, TantivyError};
use url::Url;
use zip::ZipArchive;

/// Структура для хранения информации о книге
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Book {
    id: u64, // Изменено на u64 для соответствия Tantivy
    author_name: String,
    book_title: String,
    zip_archive: String, // Относительный путь к zip-архиву
}

/// Создание схемы для Tantivy с добавленными полями `id`, `zip_archive` и `internal_file_name`
fn create_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("id", INDEXED | STORED | FAST); // Поле `id`
    schema_builder.add_text_field("author", TEXT | FAST | STORED);
    schema_builder.add_text_field("title", TEXT | FAST | STORED);
    schema_builder.add_text_field("zip_archive", TEXT | STORED); // Поле `zip_archive`
    schema_builder.build()
}

/// Открытие или создание индекса Tantivy
fn open_or_create_index(index_path: &str) -> Result<Index, TantivyError> {
    if Path::new(index_path).exists() {
        println!("Открываем существующий индекс из '{}'", index_path);
        Index::open_in_dir(index_path)
    } else {
        println!("Создаём новый индекс в '{}'", index_path);

        // Создаём директорию для индекса
        fs::create_dir_all(index_path).map_err(|e| TantivyError::from(e))?;

        let schema = create_schema();
        Index::create_in_dir(index_path, schema)
    }
}

/// Индексация данных из .inpx файла
fn build_tantivy_index<P: AsRef<Path>>(
    inpx_path: P,
    index_path: &str,
    zip_archives_dir: P,
) -> Result<(), Box<dyn Error>> {
    let index = open_or_create_index(index_path)?;
    let schema = index.schema();
    let id_field = schema.get_field("id").unwrap();
    let author_field = schema.get_field("author").unwrap();
    let title_field = schema.get_field("title").unwrap();
    let zip_archive_field = schema.get_field("zip_archive").unwrap();
    let mut writer = index.writer(50_000_000)?; // 50 MB

    let file = File::open(&inpx_path).map_err(|e| {
        format!(
            "Не удалось открыть файл '{}': {}",
            inpx_path.as_ref().display(),
            e
        )
    })?;
    let mut archive = ZipArchive::new(file).map_err(|e| {
        format!(
            "Не удалось создать ZipArchive из '{}': {}",
            inpx_path.as_ref().display(),
            e
        )
    })?;
    let mut contents_vec: Vec<String> = Vec::new();

    // Получаем директорию, где лежит inpx файл, для построения пути к zip-архивам
    let inpx_path = Path::new(inpx_path.as_ref());
    let zip_archives_dir = Path::new(zip_archives_dir.as_ref());

    // Сбор всех содержимых .inp файлов
    for i in 0..archive.len() {
        let mut inp_file = match archive.by_index(i) {
            Ok(f) => f,
            Err(e) => {
                println!(
                    "Не удалось получить файл по индексу {} в архиве '{}': {}",
                    i,
                    inpx_path.display(),
                    e
                );
                continue;
            }
        };
        if !inp_file.name().ends_with(".inp") {
            continue;
        }
        let mut contents = String::new();
        if inp_file.read_to_string(&mut contents).is_err() {
            println!(
                "Не удалось прочитать содержимое файла '{}'",
                inp_file.name()
            );
            continue;
        }
        contents_vec.push(contents);
    }

    // Последовательная обработка содержимого .inp файлов для извлечения книг
    let mut books: Vec<Book> = Vec::new();
    for (i, contents) in contents_vec.iter().enumerate() {
        for line in contents.lines() {
            let fields: Vec<&str> = line.trim_end_matches('\n').split('\x04').collect();
            if fields.len() >= 11 {
                // Убедимся, что достаточно полей
                // Извлечение `id` и построение пути к zip-архиву
                let id = match fields[5].parse::<u64>() {
                    // Используем field[5] как `id`
                    Ok(num) => num,
                    Err(e) => {
                        println!(
                            "Не удалось распарсить ID '{}' в файле {}: {}",
                            fields[5], i, e
                        );
                        continue;
                    }
                };

                // Извлечение имени .inp файла для построения имени zip-архива
                let zip_file = archive
                    .by_index(i)
                    .map_err(|e| format!("Не удалось получить файл по индексу {}: {}", i, e))?;
                let inp_file_name = zip_file.name(); // Получаем имя текущего .inp файла

                // Проверяем, что имя заканчивается на ".inp"
                if !inp_file_name.ends_with(".inp") {
                    println!("Имя файла '{}' не заканчивается на '.inp'", inp_file_name);
                    continue;
                }

                // Заменяем ".inp" на ".zip"
                let zip_file_name = inp_file_name.trim_end_matches(".inp").to_string() + ".zip";
                // Строим полный путь к zip-архиву
                let zip_archive_path = zip_archives_dir
                    .join(&zip_file_name)
                    .to_string_lossy()
                    .to_string();

                // Проверяем, существует ли zip-архив
                if !Path::new(&zip_archive_path).exists() {
                    // println!("Zip-архив '{}' не существует. Пропускаем запись с ID {}", zip_archive_path, id);
                    continue;
                }

                // Создаём структуру Book с полями `id`, `author_name`, `book_title`, `zip_archive` и `internal_file_name`
                books.push(Book {
                    id,
                    author_name: fields[0].to_string(),
                    book_title: fields[2].to_string(),
                    zip_archive: zip_archive_path,
                });
            } else {
                println!("Недостаточно полей в строке: '{}'", line);
            }
        }
    }

    println!("Индексация {} книг...", books.len());

    // Индексация каждой книги в Tantivy
    for book in books {
        let mut doc = TantivyDocument::new();
        doc.add_u64(id_field, book.id); // Добавляем `id`
        doc.add_text(author_field, &book.author_name);
        doc.add_text(title_field, &book.book_title);
        doc.add_text(zip_archive_field, &book.zip_archive); // Добавляем `zip_archive`
        writer.add_document(doc)?;
    }

    writer
        .commit()
        .map_err(|e| format!("Не удалось зафиксировать изменения в индексе: {}", e))?;
    println!("Индексация завершена и сохранена в '{}'", index_path);
    Ok(())
}

/// Поиск с использованием Tantivy, возвращает (id, author, title, score)
fn search_tantivy(
    index_path: &str,
    query_str: &str,
) -> Result<Vec<(u64, String, String, f32)>, TantivyError> {
    let index = Index::open_in_dir(index_path)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let schema = index.schema();
    let id_field = schema.get_field("id").unwrap();
    let author_field = schema.get_field("author").unwrap();
    let title_field = schema.get_field("title").unwrap();

    let query_parser = QueryParser::for_index(&index, vec![author_field, title_field]);
    let query = query_parser.parse_query(query_str)?;

    // Получение топ 10 результатов
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    let mut results = Vec::new();

    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;

        let id = retrieved_doc
            .get_first(id_field)
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let author = retrieved_doc
            .get_first(author_field)
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string();
        let title = retrieved_doc
            .get_first(title_field)
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string();
        let score = _score; // BM25 score
        results.push((id, author, title, score));
    }

    Ok(results)
}
fn get_info(index_path: &str, id: u64) -> Result<(String, String), Box<dyn Error>> {
    println!("Пытаемся скачать книгу с ID: {}", id);
    println!("Путь к индексу: {}", index_path);

    // Открываем индекс Tantivy
    let index = Index::open_in_dir(index_path)
        .map_err(|e| format!("Не удалось открыть индекс в '{}': {}", index_path, e))?;

    // Создаём ридер и поисковик
    let reader = index
        .reader()
        .map_err(|e| format!("Не удалось создать ридер для индекса: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();

    // Получаем поля из схемы
    let id_field = schema
        .get_field("id")
        .ok_or("Поле 'id' не найдено в схеме индекса")?;
    let title = schema
        .get_field("title")
        .ok_or("Поле 'zip_archive' не найдено в схеме индекса")?;
    let author = schema
        .get_field("author")
        .ok_or("Поле 'zip_archive' не найдено в схеме индекса")?;

    // Создаём запрос для конкретного `id` с использованием `Basic` опции
    let query = tantivy::query::TermQuery::new(
        tantivy::Term::from_field_u64(id_field, id),
        IndexRecordOption::Basic,
    );

    // Выполняем поиск
    let addr = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|e| format!("Ошибка при поиске ID {}: {}", id, e))?
        .first()
        .ok_or("С индексом проблемы!")?
        .1;
    let retrieved_doc = searcher.doc(addr)?;
    let title_str = retrieved_doc.get_first(title).and_then(|v| v.as_text()).ok_or("Поле 'title' отсутствует в документе")?.to_string();
    let author_str = retrieved_doc.get_first(author).and_then(|v| v.as_text()).ok_or("Поле 'title' отсутствует в документе")?.to_string();
    Ok((title_str, author_str))
}
/// Функция для скачивания книги по `id` с подробными сообщениями об ошибках
fn download_file(index_path: &str, id: u64) -> Result<bool, Box<dyn Error>> {
    println!("Пытаемся скачать книгу с ID: {}", id);
    println!("Путь к индексу: {}", index_path);

    // Открываем индекс Tantivy
    let index = Index::open_in_dir(index_path)
        .map_err(|e| format!("Не удалось открыть индекс в '{}': {}", index_path, e))?;

    // Создаём ридер и поисковик
    let reader = index
        .reader()
        .map_err(|e| format!("Не удалось создать ридер для индекса: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();

    // Получаем поля из схемы
    let id_field = schema
        .get_field("id")
        .ok_or("Поле 'id' не найдено в схеме индекса")?;
    let zip_archive_field = schema
        .get_field("zip_archive")
        .ok_or("Поле 'zip_archive' не найдено в схеме индекса")?;
    // let internal_file_name_field = schema
    //     .get_field("internal_file_name")
    //     .ok_or("Поле 'internal_file_name' не найдено в схеме индекса")?;

    // Создаём запрос для конкретного `id` с использованием `Basic` опции
    let query = tantivy::query::TermQuery::new(
        tantivy::Term::from_field_u64(id_field, id),
        IndexRecordOption::Basic,
    );

    // Выполняем поиск
    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|e| format!("Ошибка при поиске ID {}: {}", id, e))?;

    if let Some((_score, doc_address)) = top_docs.first() {
        // Получаем документ
        let retrieved_doc = searcher.doc(*doc_address).map_err(|e| {
            format!(
                "Не удалось получить документ по адресу {:?}: {}",
                doc_address, e
            )
        })?;

        // Извлекаем путь к zip-архиву и имя файла внутри архива
        let zip_archive_str = retrieved_doc
            .get_first(zip_archive_field)
            .and_then(|v| v.as_text())
            .ok_or("Поле 'zip_archive' отсутствует в документе")?;

        let ifn = format!("{}.fb2", id);
        let internal_file_name = ifn.as_str();

        println!("Найден путь к zip-архиву: {}", zip_archive_str);
        println!("Найдено имя файла внутри архива: {}", internal_file_name);

        // Создаём PathBuf из строки пути к архиву
        let zip_archive_path = PathBuf::from(zip_archive_str);

        // Проверяем существование zip-архива
        if !zip_archive_path.exists() {
            return Err(format!("Zip-архив '{}' не существует", zip_archive_path.display()).into());
        }

        // Преобразуем путь к архиву в канонический (полный) путь с правильными разделителями
        let zip_archive_canonical = zip_archive_path.canonicalize().map_err(|e| {
            format!(
                "Не удалось получить канонический путь для '{}': {}",
                zip_archive_path.display(),
                e
            )
        })?;

        // Преобразуем путь к архиву в URL
        let zip_archive_url = Url::from_file_path(&zip_archive_canonical).map_err(|_| {
            format!(
                "Не удалось преобразовать путь '{}' в URL",
                zip_archive_canonical.display()
            )
        })?;

        // Создаём экземпляр PartialZip
        let pz = PartialZip::new(&zip_archive_url)
            .map_err(|e| format!("Не удалось создать PartialZip: {}", e))?;

        // Проверяем, существует ли файл внутри архива
        let files_list = pz.list_names();
        if !files_list.contains(&internal_file_name.to_string()) {
            return Err(format!(
                "Файл '{}' не найден в архиве '{}'",
                internal_file_name, zip_archive_str
            )
            .into());
        }

        // Определяем имя выходного файла
        let output_file_name = format!("{}.fb2", id);
        let output_path = Path::new(".").join(&output_file_name);

        println!(
            "Извлечение файла '{}' в '{}'",
            internal_file_name, output_file_name
        );

        // Открываем выходной файл
        let mut output_file = File::create(&output_path).map_err(|e| {
            format!(
                "Не удалось создать выходной файл '{}': {}",
                output_path.display(),
                e
            )
        })?;

        // Извлекаем файл и записываем его в выходной файл
        pz.download_to_write(internal_file_name, &mut output_file)
            .map_err(|e| {
                format!(
                    "Ошибка при извлечении файла '{}': {}",
                    internal_file_name, e
                )
            })?;

        println!(
            "Файл '{}' успешно извлечён в '{}'",
            internal_file_name, output_file_name
        );
        Ok(true)
    } else {
        // Если документ с данным `id` не найден
        Err(format!("Книга с ID {} не найдена в индексе '{}'", id, index_path).into())
    }
}
fn get_file_bytes(index_path: &str, id: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    println!("Пытаемся скачать книгу с ID: {}", id);
    println!("Путь к индексу: {}", index_path);

    // Открываем индекс Tantivy
    let index = Index::open_in_dir(index_path)
        .map_err(|e| format!("Не удалось открыть индекс в '{}': {}", index_path, e))?;

    // Создаём ридер и поисковик
    let reader = index
        .reader()
        .map_err(|e| format!("Не удалось создать ридер для индекса: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();

    // Получаем поля из схемы
    let id_field = schema
        .get_field("id")
        .ok_or("Поле 'id' не найдено в схеме индекса")?;
    let zip_archive_field = schema
        .get_field("zip_archive")
        .ok_or("Поле 'zip_archive' не найдено в схеме индекса")?;
    // let internal_file_name_field = schema
    //     .get_field("internal_file_name")
    //     .ok_or("Поле 'internal_file_name' не найдено в схеме индекса")?;

    // Создаём запрос для конкретного `id` с использованием `Basic` опции
    let query = tantivy::query::TermQuery::new(
        tantivy::Term::from_field_u64(id_field, id),
        IndexRecordOption::Basic,
    );

    // Выполняем поиск
    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|e| format!("Ошибка при поиске ID {}: {}", id, e))?;
    let (_score, doc_address) = top_docs
        .first()
        .ok_or_else(|| format!("Книга с ID {} не найдена в индексе '{}'", id, index_path))?;
    // Получаем документ
    let retrieved_doc = searcher.doc(*doc_address).map_err(|e| {
        format!(
            "Не удалось получить документ по адресу {:?}: {}",
            doc_address, e
        )
    })?;

    // Извлекаем путь к zip-архиву и имя файла внутри архива
    let zip_archive_str = retrieved_doc
        .get_first(zip_archive_field)
        .and_then(|v| v.as_text())
        .ok_or("Поле 'zip_archive' отсутствует в документе")?;

    let ifn = format!("{}.fb2", id);
    let internal_file_name = ifn.as_str();

    println!("Найден путь к zip-архиву: {}", zip_archive_str);
    println!("Найдено имя файла внутри архива: {}", internal_file_name);

    // Создаём PathBuf из строки пути к архиву
    let zip_archive_path = PathBuf::from(zip_archive_str);

    // Проверяем существование zip-архива
    if !zip_archive_path.exists() {
        return Err(format!("Zip-архив '{}' не существует", zip_archive_path.display()).into());
    }

    // Преобразуем путь к архиву в канонический (полный) путь с правильными разделителями
    let zip_archive_canonical = zip_archive_path.canonicalize().map_err(|e| {
        format!(
            "Не удалось получить канонический путь для '{}': {}",
            zip_archive_path.display(),
            e
        )
    })?;

    // Преобразуем путь к архиву в URL
    let zip_archive_url = Url::from_file_path(&zip_archive_canonical).map_err(|_| {
        format!(
            "Не удалось преобразовать путь '{}' в URL",
            zip_archive_canonical.display()
        )
    })?;

    // Создаём экземпляр PartialZip
    let pz = PartialZip::new(&zip_archive_url)
        .map_err(|e| format!("Не удалось создать PartialZip: {}", e))?;

    // Проверяем, существует ли файл внутри архива
    let files_list = pz.list_names();
    if !files_list.contains(&internal_file_name.to_string()) {
        return Err(format!(
            "Файл '{}' не найден в архиве '{}'",
            internal_file_name, zip_archive_str
        )
        .into());
    }
    let mut buffer = Vec::new();
    pz.download_to_write(internal_file_name, &mut buffer)
        .map_err(|e| {
            format!(
                "Ошибка при извлечении файла '{}': {}",
                internal_file_name, e
            )
        })?;

    // Вся логика остается той же, но вместо записи в файл
    // Вы создаете буфер в памяти и записываете данные туда
    Ok(buffer)
}

/// Структура для инициализации и управления индексом
#[pyclass]
struct FlibRS {
    index_path: String,
    zip_archives_dir: String, // Добавлено поле для директории с архивами
}

#[pymethods]
impl FlibRS {
    #[new]
    fn new(index_path: String, zip_archives_dir: Option<String>) -> Self {
        FlibRS {
            index_path,
            zip_archives_dir: zip_archives_dir.unwrap_or_else(|| "./archive".to_string()), // Устанавливаем значение по умолчанию
        }
    }

    /// Проверяет, существует ли индекс
    fn index_exists(&self) -> bool {
        Path::new(&self.index_path).exists()
    }

    /// Построение индекса из .inpx файла
    fn build_index(&self, inpx_path: String) -> PyResult<()> {
        build_tantivy_index(&inpx_path, &self.index_path, &self.zip_archives_dir)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }

    /// Поиск по запросу, возвращает список кортежей (id, author, title, score)
    fn search(&self, query: String) -> PyResult<Vec<(u64, String, String, f32)>> {
        search_tantivy(&self.index_path, &query)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }

    /// Скачивание книги по `id`
    fn download(&self, id: u64) -> PyResult<bool> {
        download_file(&self.index_path, id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }
    fn get_file_bytes(&self, id: u64) -> PyResult<Vec<u8>> {
        get_file_bytes(&self.index_path, id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }
    fn get_info(&self, id: u64) -> PyResult<(String, String)> {
        get_info(&self.index_path, id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))
    }
}

#[pymodule]
fn flib_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FlibRS>()?;
    Ok(())
}
