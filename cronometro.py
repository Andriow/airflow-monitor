import time

class Cronometro():
    def __init__(self, logger=None):
        self.logger = logger
        self.start_time = time.time()

    def __del__(self):
        if self.logger is None:
            print(
                f"Processo finalizado em {round(time.time() - self.start_time, 4)} segundos")
        else:
            self.logger.info(
                f"Processo finalizado em {round(time.time() - self.start_time, 4)} segundos")