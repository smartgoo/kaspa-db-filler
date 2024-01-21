import logging

from .conf import conf

def configure_logger():
    logging.basicConfig(
        format="%(asctime)s::%(levelname)s::%(name)s || %(message)s",
        level=logging.DEBUG if conf.DEBUG else logging.INFO,
        handlers=[
            logging.StreamHandler()
        ]
    )