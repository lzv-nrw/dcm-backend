from dcm_backend import app_factory, config

app = app_factory(config.AppConfig())
