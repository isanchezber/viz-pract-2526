import pandas as pd
from plotnine import *

# 1. Leer el "contrato" (el CSV que Dagster ya certificó)
df = pd.read_csv("./data/poblacion_limpia.csv")

# 2. Tu laboratorio de diseño
# Aquí es donde cambias colores, escalas y facetas
p = (
    ggplot(df)
    + aes(x='Municipio', y='OBS_VALUE', fill='Medida')
    + geom_col()
    + facet_wrap('~ISLA+Anio', scales='free_y') # 'free_y' por si un año tiene valores muy distintos
    + theme_minimal()
    + theme(
        axis_text_x=element_text(rotation=90, size=6),
        figure_size=(14, 8)
    )
    + labs(title="Análisis de Renta en Municipios de Canarias")
)



# 3. Guardamos el archivo (esto crea el PNG en tu carpeta)
p.save(
    filename="grafico_renta_final.png", 
    format="png", 
    width=14,      # Ancho en pulgadas
    height=10,     # Alto en pulgadas
    dpi=300        # Calidad de impresión (alta resolución)
)
p= (ggplot(df)
    + aes(x='Municipio', y='OBS_VALUE', fill='Medida') # El año da el color
    + geom_col(position='dodge') # Las barras de años se ponen una al lado de otra
    + coord_flip()
    + theme_minimal()
    + labs(fill="Anio"))

p.save(
    filename="grafico_renta_final-2.png", 
    format="png", 
    width=14,      # Ancho en pulgadas
    height=10,     # Alto en pulgadas
    dpi=300        # Calidad de impresión (alta resolución)
)
print("¡Archivo guardado con éxito como grafico_renta_final.png!")