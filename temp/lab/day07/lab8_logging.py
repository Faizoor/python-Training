import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger('day07_lab')

def pipeline_step(name):
    try:
        logger.info('starting %s', name)
        # simulate work
        logger.info('%s completed', name)
    except Exception:
        logger.exception('%s failed', name)

if __name__ == '__main__':
    pipeline_step('sample')
