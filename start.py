
from congregate import Congregate

def start():
    """
    Start Congregate Node

    """
    try:
        c = Congregate()
        try:
            try:
                asyncio.get_event_loop().run_forever()
            except Exception as e:
                logger.debug(e)
        except KeyboardInterrupt:
            c.shutdown()
            print('done')
        finally:
            asyncio.get_event_loop().close()
    except Exception as e:
        logger.debug(e)

start()       
