import sqlite3

# Подключение к БД
conn = sqlite3.connect('business_messages.db')
cursor = conn.cursor()

# Найти все "video" файлы с именем *video_note* или *videonote*
cursor.execute("""
SELECT message_id, chat_id, owner_id, media_path 
FROM messages 
WHERE media_type = 'video' 
AND (media_path LIKE '%video_note%' OR media_path LIKE '%videonote%')
""")

rows = cursor.fetchall()
print(f"Найдено {len(rows)} кружков, сохраненных как 'video'")

if rows:
    # Исправить тип на video_note
    for row in rows:
        msg_id, chat_id, owner_id, path = row
        print(f"  Исправляю: {path}")
    
    cursor.execute("""
    UPDATE messages 
    SET media_type = 'video_note' 
    WHERE media_type = 'video' 
    AND (media_path LIKE '%video_note%' OR media_path LIKE '%videonote%')
    """)
    
    conn.commit()
    print(f"✅ Исправлено: {cursor.rowcount} записей")
else:
    print("Нет записей для исправления")

# Аналогично для saved_
cursor.execute("""
UPDATE messages 
SET media_type = 'saved_video_note' 
WHERE media_type = 'saved_video' 
AND (media_path LIKE '%video_note%' OR media_path LIKE '%videonote%')
""")

if cursor.rowcount > 0:
    conn.commit()
    print(f"✅ Исправлено saved: {cursor.rowcount} записей")

conn.close()
