import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import pandas as pd
from docx import Document

# Настройка доступа к Google API
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

def authenticate_google_apis(credentials_file):
    """Аутентификация в Google API"""
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_client = gspread.authorize(creds)
    return drive_service, sheets_client

def get_files_from_folder(drive_service, folder_id):
    """Получение списка файлов из папки Google Drive"""
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query,
        pageSize=1000,
        fields="files(id, name, mimeType)"
    ).execute()
    
    files = results.get('files', [])
    print(f"Найдено файлов в папке: {len(files)}")
    
    # Создаем словарь для быстрого поиска по имени файла
    files_dict = {}
    for file in files:
        # Убираем расширение файла для сравнения
        name_without_extension = file['name'].split('.')[0]
        files_dict[name_without_extension] = file
        files_dict[file['name']] = file  # Добавляем также полное имя
    
    return files_dict

def get_names_from_spreadsheet(sheets_client, spreadsheet_id):
    """Получение имен из столбцов B, D, E таблицы"""
    try:
        spreadsheet = sheets_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.sheet1
        
        # Получаем все данные из столбцов B, D, E
        column_b = worksheet.col_values(2)  # Столбец B
        column_d = worksheet.col_values(4)  # Столбец D  
        column_e = worksheet.col_values(5)  # Столбец E
        
        # Объединяем все имена и убираем дубликаты
        all_names = set()
        
        # Добавляем имена из столбца B (пропускаем заголовок если есть)
        for name in column_b[1:]:  # пропускаем первую строку (заголовок)
            if name.strip():
                all_names.add(name.strip())
        
        # Добавляем имена из столбца D
        for name in column_d[1:]:
            if name.strip():
                all_names.add(name.strip())
        
        # Добавляем имена из столбца E
        for name in column_e[1:]:
            if name.strip():
                all_names.add(name.strip())
        
        print(f"Найдено уникальных имен в таблице: {len(all_names)}")
        return list(all_names)
        
    except Exception as e:
        print(f"Ошибка при чтении таблицы: {str(e)}")
        return []

def read_docx_content(file_content):
    """Чтение содержимого из DOCX файла"""
    try:
        # Создаем временный файл в памяти
        doc_file = io.BytesIO(file_content)
        doc = Document(doc_file)
        
        # Извлекаем текст из всех параграфов
        full_text = []
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                full_text.append(paragraph.text.strip())
        
        # Извлекаем текст из таблиц
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    if cell.text.strip():
                        full_text.append(cell.text.strip())
        
        return '\n'.join(full_text)
        
    except Exception as e:
        print(f"Ошибка при чтении DOCX: {str(e)}")
        return f"Ошибка чтения DOCX: {str(e)}"

def download_file_content(drive_service, file_id, file_name):
    """Скачивание содержимого файла"""
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        content = file_stream.getvalue()
        
        # Обработка DOCX файлов
        if file_name.lower().endswith('.docx'):
            return read_docx_content(content)
        else:
            # Для других типов файлов (на всякий случай)
            return f"Неподдерживаемый формат файла: {file_name}"
            
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_name}: {str(e)}")
        return f"Ошибка загрузки: {str(e)}"

def find_matching_files(files_dict, names_to_find):
    """Поиск файлов, соответствующих именам из таблицы"""
    matches = {}
    
    for name in names_to_find:
        # Пробуем найти точное совпадение (без расширения)
        if name in files_dict:
            matches[name] = files_dict[name]
            continue
            
        # Пробуем найти совпадение без учета регистра
        name_lower = name.lower()
        for file_name, file_data in files_dict.items():
            file_base_name = file_name.split('.')[0].lower()
            if name_lower == file_base_name:
                matches[name] = file_data
                break
        
        # Если не нашли, ищем частичное совпадение
        if name not in matches:
            for file_name, file_data in files_dict.items():
                file_base_name = file_name.split('.')[0].lower()
                if name_lower in file_base_name or file_base_name in name_lower:
                    matches[name] = file_data
                    print(f"Найдено частичное совпадение: {name} -> {file_name}")
                    break
    
    print(f"Найдено соответствий файлов: {len(matches)}")
    return matches

def update_spreadsheet_with_content(sheets_client, spreadsheet_id, matches, drive_service):
    """Обновление таблицы - добавление содержимого файлов в столбец Z"""
    try:
        spreadsheet = sheets_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.sheet1
        
        # Получаем все данные для поиска соответствий
        all_data = worksheet.get_all_values()
        
        # Создаем словарь для быстрого доступа к содержимому файлов
        file_contents = {}
        for name, file_data in matches.items():
            print(f"Загружаем содержимое файла: {file_data['name']}")
            content = download_file_content(drive_service, file_data['id'], file_data['name'])
            # Ограничиваем длину содержимого (Google Sheets имеет ограничения)
            if len(content) > 50000:
                content = content[:50000] + "... [обрезано]"
            file_contents[name] = content
        
        updates = []
        # Проходим по всем строкам таблицы (начиная со 2-й, если 1-я - заголовок)
        for row_idx, row in enumerate(all_data[1:], start=2):  # start=2 потому что в gspread строки с 1
            current_name = None
            
            # Проверяем столбцы B, D, E (индексы 1, 3, 4 в 0-based)
            for col_idx in [1, 3, 4]:
                if col_idx < len(row) and row[col_idx].strip():
                    name = row[col_idx].strip()
                    if name in file_contents:
                        current_name = name
                        break
            
            if current_name:
                # Обновляем столбец Z (индекс 25 в 0-based)
                updates.append({
                    'range': f'Z{row_idx}',
                    'values': [[file_contents[current_name]]]
                })
        
        # Применяем все обновления batch-запросом
        if updates:
            worksheet.batch_update(updates)
            print(f"Обновлено {len(updates)} строк в столбце Z")
        else:
            print("Нет данных для обновления")
            
    except Exception as e:
        print(f"Ошибка при обновлении таблицы: {str(e)}")

def main():
    # Конфигурация
    FOLDER_ID = '1iBe9GhWoLBmjXdGIZAZtp5mSwn1pxqVZ'
    SPREADSHEET_ID = '1VJxZN37-vaeWZUQyBHF01gRvfA8PAhumgAycAr10_so'
    CREDENTIALS_FILE = 'hallowed-index-473614-a7-6cf3f410fdef'  # Ваш файл с учетными данными
    
    try:
        # Аутентификация
        print("Аутентификация в Google API...")
        drive_service, sheets_client = authenticate_google_apis(CREDENTIALS_FILE)
        
        # Получение файлов из папки
        print("Получение списка файлов из Google Drive...")
        files_dict = get_files_from_folder(drive_service, FOLDER_ID)
        
        # Выводим список найденных файлов для отладки
        print("Найденные файлы:")
        for file_name in list(files_dict.keys())[:10]:  # Покажем первые 10
            print(f"  - {file_name}")
        if len(files_dict) > 10:
            print(f"  ... и еще {len(files_dict) - 10} файлов")
        
        # Получение имен из таблицы
        print("Получение имен из Google таблицы...")
        names_from_sheet = get_names_from_spreadsheet(sheets_client, SPREADSHEET_ID)
        
        # Выводим несколько имен для отладки
        print("Примеры имен из таблицы:")
        for name in list(names_from_sheet)[:10]:
            print(f"  - {name}")
        
        # Поиск соответствий
        print("Поиск соответствий между именами и файлами...")
        matches = find_matching_files(files_dict, names_from_sheet)
        
        # Выводим найденные соответствия
        print("Найденные соответствия:")
        for name, file_data in matches.items():
            print(f"  - {name} -> {file_data['name']}")
        
        # Обновление таблицы
        print("Обновление Google таблицы...")
        update_spreadsheet_with_content(sheets_client, SPREADSHEET_ID, matches, drive_service)
        
        print("Готово! Данные успешно добавлены в столбец Z таблицы.")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
