from setuptools import find_packages, setup


setup(
    name="wama-alerta-mailer",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    entry_points={
        "alerta.plugins": [
            "wama_initial_episode_mailer = wama_alerta_mailer.plugin:WamaInitialEpisodeMailer",
        ],
    },
)