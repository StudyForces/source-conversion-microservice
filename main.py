import os
import pika
import json
import requests
from wand.image import Image
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
SENDER_QUEUE_NAME = os.getenv("SENDER_QUEUE_NAME")
CONSUMER_QUEUE_NAME = os.getenv("CONSUMER_QUEUE_NAME")

print(HOST, SENDER_QUEUE_NAME, CONSUMER_QUEUE_NAME)

mimeTypesExtension = {
    "image/png": "png",
    "image/jpeg": "jpg",
    "image/gif": "gif",
    "application/pdf": "pdf"
}

connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))

rmq_channel = connection.channel()
session = requests.Session()

rmq_channel.exchange_declare(exchange=SENDER_QUEUE_NAME, exchange_type='topic', durable=True)


def download_image(session: requests.Session, url: str, path: str) -> bool:
    if os.path.exists(path):
        os.remove(path)
    r = session.get(url, stream=True)
    if r.status_code == 200:
        with open(path, "wb") as f:
            f.write(r.content)
        return True
    return False


def upload_image(session: requests.Session, url: str, path: str, t=0) -> bool:
    with open(path, 'rb') as upload:
        r = session.put(url, data=upload, headers={"Content-Type": "image/png"}, stream=True)
        if r.status_code == 200:
            return True
        else:
            print(r.status_code)
            print(url, r.request.headers)
            if t < 5:
                upload_image(session, url, path, t + 1)
        return False


def send(message: str) -> None:
    print(message)
    rmq_channel.basic_publish(exchange=SENDER_QUEUE_NAME, routing_key=SENDER_QUEUE_NAME, body=message)


def on_message(channel, method_frame, header_frame, body) -> None:
    print(body)

    rmq_channel.basic_ack(method_frame.delivery_tag)
    req = json.loads(body)
    upload_id = req["sourceUploadID"]
    upload_urls = req["uploadURLs"]
    file_infos = req["fileInfos"]

    os.makedirs(f"tmp/{upload_id}", exist_ok=True)

    files = []
    counter = 0

    for idx, info in enumerate(file_infos):
        url = info["url"]
        mime_type = info["fileInfo"]["contentType"]
        file = f"tmp/{upload_id}/{idx}.{mimeTypesExtension[mime_type]}"
        if not download_image(session, url, file):
            print(f'Failed to download {file}')
            return None

        conv_path = f'tmp/conv/{file}'
        os.makedirs(conv_path, exist_ok=True)

        if mime_type == "application/pdf" or mime_type == "image/gif":
            ny = Image(filename=file, resolution=200)
            ny_converted = ny.convert('png')
            for page_number, img in enumerate(ny_converted.sequence):
                page = Image(image=img)
                page.format = 'png'
                save_path = f'{conv_path}/{page_number}.png'
                page.save(filename=save_path)
                page.destroy()
                print(f'Uploading {save_path}...')
                if upload_image(session, upload_urls[counter]["url"], save_path):
                    files.append(upload_urls[counter]["fileName"])
                    print(f'Uploaded {save_path}')
                else:
                    print(f'Something went wrong with uploading {save_path}')
                counter += 1
                os.remove(save_path)
            ny_converted.destroy()
            ny.destroy()
        else:
            ny = Image(filename=file)
            save_path = f'{conv_path}/0.png'
            ny.save(filename=save_path)
            ny.destroy()
            print(f'Uploading {save_path}...')
            if upload_image(session, upload_urls[counter]["url"], save_path):
                files.append(upload_urls[counter]["fileName"])
                print(f'Uploaded {save_path}')
            else:
                print(f'Something went wrong with uploading {save_path}')
            counter += 1
            os.remove(save_path)
        os.remove(file)
        os.removedirs(conv_path)

    res = {
        "sourceUploadID": upload_id,
        "data": {
            "files": files
        }
    }

    if counter == len(files):
        send(json.dumps(res, separators=(',', ':'), ensure_ascii=False))


result = rmq_channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

rmq_channel.queue_bind(exchange=CONSUMER_QUEUE_NAME, queue=queue_name, routing_key=CONSUMER_QUEUE_NAME)

rmq_channel.basic_consume(
    queue=queue_name, on_message_callback=on_message, auto_ack=False)

try:
    rmq_channel.start_consuming()
except KeyboardInterrupt:
    rmq_channel.close()
    session.close()
