import gdown
import datetime


def download_file():
    file_id = "1v6LRphp2kYciU4SXp0PCjEMuev1bDejc"
    url = f"https://drive.google.com/uc?id={file_id}"

    date = datetime.datetime.now().strftime('%d.%m.%Y')
    output = f"data/{date}_data.csv"
    
    gdown.download(url, output)

download_file()