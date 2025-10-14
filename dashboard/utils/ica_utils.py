ICA_CATEGORIAS = {
    "Buena": {"color": "#00e400", "rango": (0, 50)},
    "Moderada": {"color": "#ffff00", "rango": (51, 100)},
    "Dañina a grupos sensibles": {"color": "#ff7e00", "rango": (101, 150)},
    "Dañina": {"color": "#ff0000", "rango": (151, 200)},
    "Muy dañina": {"color": "#8f3f97", "rango": (201, 300)},
    "Peligrosa": {"color": "#7e0023", "rango": (301, 500)},
}

def obtener_color_por_categoria(categoria):
    return ICA_CATEGORIAS.get(categoria, {"color": "#808080"})["color"]