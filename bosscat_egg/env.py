


if __name__ == "__main__":

    import os
    import sys
    from bosscat.utils import getenv
    from deploy.bosscat_config import config_deltas
    from bosscat.upanddown import configure
    config = dict(config_deltas[sys.argv[1]])
    config['deployment_tag'] = sys.argv[2]
    config = configure(config)
    env = getenv(config, 'web')
    for key, value in env.items():
        print('export {}="{}"'.format(key, value))


