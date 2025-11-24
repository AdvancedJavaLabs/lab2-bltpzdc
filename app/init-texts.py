import os
import psycopg2

TEXT_DIR = "./texts"
CHUNK_SIZE = 1024  # 1 KB


def insert_text(conn, name):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO texts(name) VALUES (%s) RETURNING id",
            (name,)
        )
        return cur.fetchone()[0]


def insert_section(conn, text_id, section_number, content):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sections(text_id, section_number, content)
               VALUES (%s, %s, %s)""",
            (text_id, section_number, content)
        )


def main():
    conn = psycopg2.connect(
        dbname="textdb",
        user="user",
        password="password",
        host="localhost",
        port=5432
    )

    for filename in os.listdir(TEXT_DIR):

        full_path = os.path.join(TEXT_DIR, filename)
        if not os.path.isfile(full_path):
            continue

        print(f"Processing {filename}...")

        # remove extension
        name, _ = os.path.splitext(filename)

        with open(full_path, "rb") as f:
            data = f.read()

        text_id = insert_text(conn, name)

        section_number = 1
        offset = 0

        while offset < len(data):
            chunk = data[offset: offset + CHUNK_SIZE]

            chunk_str = chunk.decode("utf-8", errors="replace")

            insert_section(conn, text_id, section_number, chunk_str)

            offset += CHUNK_SIZE
            section_number += 1

        conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
