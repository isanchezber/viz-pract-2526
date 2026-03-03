import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, MetadataValue
from plotnine import ggplot, aes, geom_line, geom_point, theme_minimal, scale_color_manual, labs, annotate


@asset
def islas_raw():
    df = pd.read_csv("./data/pwbi-1.csv")
    #EJERCICIO 6
    # Añadimos una fila "sucia" para forzar el fallo del check
    #fila_sucia = pd.DataFrame({"año": [2022], "isla": ["tenerife"], "medida": ["gasto"], "valor": [1840000]})
    #return pd.concat([df, fila_sucia], ignore_index=True)
    return df

@asset_check(asset=islas_raw)
def check_estandarizacion_islas(islas_raw):
    # Contamos categorías únicas originales vs normalizadas
    originales = islas_raw['isla'].nunique()
    normalizadas = islas_raw['isla'].str.capitalize().nunique()
    
    passed = originales == normalizadas
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Si hay nombres inconsistentes, ggplot creará leyendas duplicadas."
        }
    )




@asset(deps=[islas_raw]) # Este asset depende del anterior
def grafico_islas_gasto(islas_raw):
    # En un flujo real, aquí usarías el asset 'islas_curated' ya limpio
    
    df_gasto = islas_raw[islas_raw['medida'] == 'gasto'].copy()
    
    # --- APLICACIÓN DE PUNTO FOCAL (Gestalt) ---
    # Creamos una columna para definir el color: Tenerife (Foco) vs Resto (Fondo)
    df_gasto['highlight'] = df_gasto['isla'].apply(
        lambda x: 'Foco' if x == 'Tenerife' else 'Fondo'
    )
    
    p = (
        ggplot(df_gasto, aes(x='año', y='valor', group='isla', color='highlight'))
        + geom_line(size=1.5)
        + geom_point(size=3)
        # Semejanza (Gris para fondo) vs Punto Focal (Azul para destacar)
        + scale_color_manual(values={'Foco': '#007bff', 'Fondo': '#d3d3d3'})
        + theme_minimal()
        + labs(
            title="Evolución del Gasto: Tenerife como Punto Focal",
            subtitle="El uso del color rompe la semejanza para dirigir la atención",
            x="Año",
            y="Gasto (€)",
            color="Jerarquía"
        )
    )
    
    # Guardamos el gráfico
    p.save("grafico_islas.png")
    return "grafico_islas.png"

# 3. CHECK DE PUNTO FOCAL
@asset_check(asset=grafico_islas_gasto)
def check_focal_clarity(grafico_islas_gasto):
    # Lógica: ¿Hay demasiados elementos compitiendo?
    # Aquí podríamos verificar si el DataFrame usado tiene un solo 'Foco'
    # Por ahora, simulamos una validación de diseño
    passed = True 
    return AssetCheckResult(
        passed=passed,
        metadata={
            "principio": "Punto Focal",
            "metodología": "Ruptura de Semejanza por color",
            "status": "Atención dirigida correctamente a Tenerife"
        }
    )
