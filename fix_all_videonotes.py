import sqlite3
import os

# Подключение к БД
conn = sqlite3.connect('business_messages.db')
cursor = conn.cursor()

# Получить ВСЕ записи с media_type = 'video'
cursor.execute("""
SELECT message_id, chat_id, owner_id, media_path, media_type
FROM messages 
WHERE media_type = 'video' AND media_path IS NOT NULL
""")

rows = cursor.fetchall()
print(f"Найдено {len(rows)} видео в БД")

# Проверить, какие из них - кружки (по имени файла)
videonotes_to_fix = []

for msg_id, chat_id, owner_id, media_path, media_type in rows:
    if not media_path:
        continue
    
    # Получить имя файла
    filename = os.path.basename(media_path)
    
    # Проверить, является ли это кружком
    # Кружки обычно называются: video_note_*, videonote_*, или saved_videonote_*
    is_videonote = (
        'video_note_' in filename or 
        'videonote_' in filename or
        '_videonote_' in filename
    )
    
    if is_videonote:
        videonotes_to_fix.append((msg_id, chat_id, owner_id, filename))
        print(f"  📹 Найден кружок: {filename}")

print(f"\n🔍 Найдено кружков с неправильным типом: {len(videonotes_to_fix)}")

if videonotes_to_fix:
    print("\nИсправляю...")
    
    for msg_id, chat_id, owner_id, filename in videonotes_to_fix:
        # Определить правильный тип
        if 'saved_' in filename:
            new_type = 'saved_video_note'
        else:
            new_type = 'video_note'
        
        # Обновить запись
        cursor.execute("""
        UPDATE messages 
        SET media_type = ? 
        WHERE message_id = ? AND chat_id = ? AND owner_id = ?
        """, (new_type, msg_id, chat_id, owner_id))
        
        print(f"  ✅ {filename} -> {new_type}")
    
    conn.commit()
    print(f"\n✅ Исправлено: {len(videonotes_to_fix)} записей")
else:
    print("\n✅ Нет записей для исправления")

# Проверка после исправления
cursor.execute("SELECT DISTINCT media_type FROM messages WHERE media_type LIKE '%video%'")
print("\n📊 Типы медиа в БД (после):")
for row in cursor.fetchall():
    cursor.execute(f"SELECT COUNT(*) FROM messages WHERE media_type = ?", (row[0],))
    count = cursor.fetchone()[0]
    print(f"  - '{row[0]}': {count} шт.")

conn.close()
