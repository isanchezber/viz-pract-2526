from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules, define_asset_job, AssetSelection
from scripts import test_prompt
from scripts.sensor_test import my_directory_sensor


assets_prompt=load_assets_from_modules([test_prompt])
# 2. Definimos el Job (la unión de los assets)
pipeline_ia_job = define_asset_job(
    name="job_visualizacion_ia",
    selection=AssetSelection.all()
)


# 3. Empaquetamos TODO para Dagster
defs = Definitions(
    assets=assets_prompt,
    jobs=[pipeline_ia_job],
    sensors=[my_directory_sensor]
    # Si tuvieras schedules, también irían aquí
)