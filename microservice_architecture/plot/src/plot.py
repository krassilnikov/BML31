import pandas as pd
import matplotlib.pyplot as plt
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os

# Константы для имени файлов
LOG_FILE = './logs/plot_log.txt'
PNG_FILE = './logs/error_distribution.png'
CSV_FILE = './logs/metric_log.csv'

def log(message):
    """Записывает сообщения в лог-файл."""
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

class CSVEventHandler(FileSystemEventHandler):
    """Обработчик событий для изменения CSV файла."""
    def __init__(self, csv_file):
        super().__init__()
        self.csv_file = csv_file

    def on_modified(self, event):
        """Срабатывает при изменении файла."""
        if event.src_path == self.csv_file:
            log(f"{self.csv_file} изменен. Перестраиваю гистограмму...")
            self.plot_histogram()

    def plot_histogram(self):
        """Строит гистограмму абсолютной ошибки из CSV файла."""
        df = pd.read_csv(self.csv_file)
        
        plt.figure(figsize=(10, 6))
        plt.hist(df['absolute_error'], bins=20, color='blue', edgecolor="black", alpha=0.7)
        plt.title('Гистограмма абсолютной ошибки')
        plt.xlabel('Абсолютная ошибка')
        plt.ylabel('Частота')
        plt.grid(axis='y', alpha=0.75)
        
        plt.savefig(PNG_FILE)
        plt.close()
        log(f"Гистограмма сохранена в {PNG_FILE}")

def monitor_csv(csv_file):
    """Запускает наблюдение за изменениями в CSV файле."""
    event_handler = CSVEventHandler(csv_file)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(csv_file), recursive=False)
    observer.start()
    log(f"Начато отслеживание изменений в {csv_file}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def main():
    with open(LOG_FILE, 'w') as logfile:
        logfile.write('Plotter is alive\n')

    while True:
        log("Plotter restarted")
        try:
            time.sleep(10)
            monitor_csv(CSV_FILE)
        except Exception as e:
            log(f"Ошибка при построении графика: {e}")

if __name__ == "__main__":
    main()