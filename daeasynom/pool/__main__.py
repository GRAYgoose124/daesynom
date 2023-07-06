import logging
import time

from .pool import QueueingThreadPool


class MyQTP(QueueingThreadPool):
    max_jobs = 100
    num_threads = 4

    class Actions:
        @staticmethod
        def pprint(*args):
            print(*args)

        @staticmethod
        def plen(*args):
            return len(*args)

        @staticmethod
        def pstr(*args):
            return str(*args)

        @staticmethod
        def psum(*args):
            return sum(*args)


def main():
    logging.basicConfig(level=logging.DEBUG)

    qtp = MyQTP()
    qtp.start()

    try:
        running = True
        while running:
            qtp.submit_work("pprint", ("Hello",))
            qtp.submit_work("plen", ("Hello",))
            qtp.submit_work("pstr", (123,))
            qtp.submit_work("psum", ([1, 2, 3],))
            try:
                qtp.submit_work("notavalidrequest", ([1, 2, 3, 4],))
            except Exception as e:
                print(e)
                break

            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        qtp.join()


if __name__ == "__main__":
    main()
