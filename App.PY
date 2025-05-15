import base64
import io
import os
from datetime import datetime, timedelta
import enum

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Enum, func
from sqlalchemy.orm import sessionmaker, relationship, joinedload
from sqlalchemy.ext.declarative import declarative_base

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import sqlite3

# --- Configuración base de datos y modelos SQLAlchemy ---

engine = create_engine('sqlite:///datos2.db', echo=False)
Base = declarative_base()

class CanalEnum(enum.Enum):
    uh = 'uh'
    ex = 'ex'
    dis = 'dis'
    es = 'es'
    imp = 'imp'

class Unidad(Base):
    __tablename__ = 'unidad'
    gfh = Column(String, primary_key=True)
    tipo = Column(String(10), nullable=False)
    denominacion = Column(String, nullable=False)

class Producto(Base):
    __tablename__ = 'producto'
    espec = Column(String(10), primary_key=True)
    nombre = Column(String(50), nullable=False)

class Movimiento(Base):
    __tablename__ = 'movimiento'
    id = Column(Integer, primary_key=True, autoincrement=True)
    unidad_gfh = Column(String, ForeignKey('unidad.gfh'), nullable=False)
    producto_espec = Column(String, ForeignKey('producto.espec'), nullable=False)
    tipo_e_s = Column(Enum('E', 'S'), nullable=False)
    fecha = Column(DateTime, default=datetime.utcnow)
    unidad = relationship("Unidad", backref="movimientos")
    producto = relationship("Producto", backref="movimientos")

class DetalleMovimiento(Base):
    __tablename__ = 'detalle_movimiento'
    id = Column(Integer, primary_key=True, autoincrement=True)
    movimiento_id = Column(Integer, ForeignKey('movimiento.id'), nullable=False)
    canal = Column(Enum(CanalEnum), nullable=False)
    unidades = Column(Integer, nullable=True)
    pml = Column(sqlalchemy.types.DECIMAL(15, 4), nullable=True)
    pvf = Column(sqlalchemy.types.DECIMAL(15, 4), nullable=True)
    pmf = Column(sqlalchemy.types.DECIMAL(15, 4), nullable=True)
    pvl = Column(sqlalchemy.types.DECIMAL(15, 4), nullable=True)
    pvp = Column(sqlalchemy.types.DECIMAL(15, 4), nullable=True)
    movimiento = relationship("Movimiento", backref="detalles")

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# --- Función para insertar datos desde un buffer Excel ---

def insertar_datos_desde_excel_buffer(buffer, ruta_db):
    try:
        df = pd.read_excel(buffer)
        df.columns = df.columns.str.strip().str.lower()

        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()

        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS unidad (
            tipo         TEXT NOT NULL,
            denominacion TEXT NOT NULL,
            gfh          TEXT NOT NULL UNIQUE PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS producto (
            espec  TEXT NOT NULL UNIQUE PRIMARY KEY,
            nombre TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS movimiento (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            unidad_gfh     TEXT NOT NULL,
            producto_espec TEXT NOT NULL,
            tipo_e_s       TEXT CHECK (tipo_e_s IN ('E', 'S')) NOT NULL,
            fecha          DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (unidad_gfh) REFERENCES unidad (gfh),
            FOREIGN KEY (producto_espec) REFERENCES producto (espec)
        );

        CREATE TABLE IF NOT EXISTS detalle_movimiento (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            movimiento_id INTEGER NOT NULL,
            canal         TEXT CHECK (canal IN ('uh', 'ex', 'dis', 'es', 'imp')) NOT NULL,
            unidades      INTEGER,
            pml           DECIMAL(15, 4),
            pvf           DECIMAL(15, 4),
            pmf           DECIMAL(15, 4),
            pvl           DECIMAL(15, 4),
            pvp           DECIMAL(15, 4),
            FOREIGN KEY (movimiento_id) REFERENCES movimiento(id)
        );
        """)
        conn.commit()

        unidades = df[['tipo', 'denominacion', 'gfh']].drop_duplicates()
        cursor.executemany("""
            INSERT OR IGNORE INTO unidad (tipo, denominacion, gfh)
            VALUES (?, ?, ?)
        """, unidades.itertuples(index=False, name=None))
        conn.commit()

        productos = df[['espec', 'registrado']].drop_duplicates()
        cursor.executemany("""
            INSERT OR IGNORE INTO producto (espec, nombre)
            VALUES (?, ?)
        """, productos.itertuples(index=False, name=None))
        conn.commit()

        movimientos = df[['gfh', 'espec', 'tipo_e_s']].copy()
        cursor.executemany("""
            INSERT INTO movimiento (unidad_gfh, producto_espec, tipo_e_s)
            VALUES (?, ?, ?)
        """, movimientos.itertuples(index=False, name=None))
        conn.commit()

        num_rows = len(movimientos)
        rows = cursor.execute("SELECT id FROM movimiento ORDER BY id DESC LIMIT ?", (num_rows,)).fetchall()
        movimiento_ids = [r[0] for r in reversed(rows)]

        canales = ['uh', 'ex', 'dis', 'es', 'imp']
        detalle_data = []

        for canal in canales:
            detalle_temp = pd.DataFrame({
                'movimiento_id': movimiento_ids,
                'canal': canal,
                'unidades': df.get(f'unidades_{canal}', pd.Series([None]*len(df))),
                'pml': df.get(f'pml_{canal}', pd.Series([None]*len(df))),
                'pmf': df.get(f'pmf_{canal}', pd.Series([None]*len(df))),
                'pvl': df.get(f'pvl_{canal}', pd.Series([None]*len(df))),
                'pvp': df.get(f'pvp_{canal}', pd.Series([None]*len(df))),
                'pvf': df.get(f'pvf_{canal}', pd.Series([None]*len(df))),
            })
            detalle_data.append(detalle_temp)

        detalles_final = pd.concat(detalle_data, ignore_index=True)

        cursor.executemany("""
            INSERT INTO detalle_movimiento (
                movimiento_id, canal, unidades, pml, pmf, pvl, pvp, pvf
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, detalles_final.itertuples(index=False, name=None))
        conn.commit()

        conn.close()
        return "✅ Inserción completada correctamente."

    except Exception as e:
        return f"❌ Error durante la inserción: {e}"

# --- Funciones para mostrar tabla y detalle ---

def get_table_data_detalle(year_filter=None, start_date=None, end_date=None):
    session = Session()
    query = session.query(DetalleMovimiento, Movimiento.fecha).join(Movimiento, Movimiento.id == DetalleMovimiento.movimiento_id)

    if year_filter and year_filter != 'Todos':
        query = query.filter(func.strftime('%Y', Movimiento.fecha) == str(year_filter))

    if start_date and end_date:
        query = query.filter(Movimiento.fecha >= start_date, Movimiento.fecha <= end_date)

    results = query.all()
    session.close()

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame([{
        'id': item.DetalleMovimiento.id,
        'movimiento_id': item.DetalleMovimiento.movimiento_id,
        'canal': item.DetalleMovimiento.canal.name,
        'unidades': item.DetalleMovimiento.unidades,
        'pml': float(item.DetalleMovimiento.pml) if item.DetalleMovimiento.pml else None,
        'pmf': float(item.DetalleMovimiento.pmf) if item.DetalleMovimiento.pmf else None,
        'pvl': float(item.DetalleMovimiento.pvl) if item.DetalleMovimiento.pvl else None,
        'pvp': float(item.DetalleMovimiento.pvp) if item.DetalleMovimiento.pvp else None,
        'fecha_movimiento': item.fecha.strftime('%Y-%m-%d')
    } for item in results])

    return df

def get_movimiento_detail(movimiento_id):
    session = Session()
    mov = session.query(Movimiento).options(
        joinedload(Movimiento.unidad),
        joinedload(Movimiento.producto)
    ).filter(Movimiento.id == movimiento_id).first()
    session.close()

    if not mov:
        return None

    detalle = {
        "ID": mov.id,
        "Unidad GFH": mov.unidad_gfh,
        "Unidad Denominacion": mov.unidad.denominacion if mov.unidad else "N/A",
        "Unidad Tipo": mov.unidad.tipo if mov.unidad else "N/A",
        "Producto Espec": mov.producto_espec,
        "Producto Nombre": mov.producto.nombre if mov.producto else "N/A",
        "Tipo E/S": mov.tipo_e_s,
        "Fecha": mov.fecha.strftime('%Y-%m-%d %H:%M:%S')
    }
    return detalle

# --- App Dash y layout ---

app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    html.H1("Detalle Movimiento"),

    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Arrastra o selecciona un archivo Excel para cargar.'
        ]),
        style={
            'width': '50%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        multiple=False
    ),

    html.Div(id='output-upload'),

    html.Div([
        html.Label("Seleccionar Año:"),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': 'Ver todos', 'value': 'Todos'}] + [{'label': y, 'value': y} for y in sorted(set([d.year for d in pd.date_range('2020-01-01', datetime.today())]), reverse=True)],
            value='Todos',
            placeholder="Seleccionar un año"
        )
    ], style={'padding': '20px'}),

    html.Div([
        html.Label("Seleccionar Rango de Fechas:"),
        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=datetime(2020, 1, 1),
            end_date=datetime.today(),
            display_format='YYYY-MM-DD',
            style={'padding': '20px'}
        )
    ]),

    html.Div([
        html.Label("Filtrar por Rango:"),
        dcc.Checklist(
            id='filter-range-checklist',
            options=[
                {'label': 'Este Año', 'value': 'este_año'},
                {'label': 'Últimos 12 Meses', 'value': 'ultimos_12_meses'}
            ],
            value=[],
            inline=True
        )
    ], style={'padding': '20px'}),

    html.Div(id='tabla-contenedor', style={'padding': '20px'}),

    html.Div(id='detalle-movimiento', style={'padding': '20px', 'border': '1px solid #ccc', 'margin-top': '20px'})
])

# --- Callbacks ---

@app.callback(
    Output('output-upload', 'children'),
    Input('upload-data', 'contents'),
    State('upload-data', 'filename'),
)
def procesar_archivo_subido(contents, filename):
    if contents is None:
        return ""
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    buffer = io.BytesIO(decoded)

    ruta_db = 'datos2.db'  # Ruta de la base de datos

    resultado = insertar_datos_desde_excel_buffer(buffer, ruta_db)
    return html.Div([
        html.P(f"Archivo cargado: {filename}"),
        html.P(resultado)
    ])


@app.callback(
    Output('tabla-contenedor', 'children'),
    Input('year-dropdown', 'value'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input('filter-range-checklist', 'value')
)
def actualizar_tabla(year_filter, start_date, end_date, range_filter):
    filtros_aplicados = False

    if year_filter and year_filter != 'Todos':
        filtros_aplicados = True

    if range_filter and len(range_filter) > 0:
        filtros_aplicados = True

    if not filtros_aplicados:
        # Mostrar TODO sin filtrar, ignorando fechas
        df = get_table_data_detalle(year_filter=None, start_date=None, end_date=None)
    else:
        # Aplicar filtros de año y checklist
        start_date_dt = None
        end_date_dt = None

        # Si el checklist tiene rango especial, usarlo
        if 'este_año' in range_filter:
            start_date_dt = datetime(datetime.now().year, 1, 1)
            end_date_dt = datetime.today()
        elif 'ultimos_12_meses' in range_filter:
            end_date_dt = datetime.today()
            start_date_dt = end_date_dt - timedelta(days=365)
        else:
            # Si no hay checklist que defina rango, tomar rango solo si ambos dates están presentes
            if start_date and end_date:
                start_date_dt = datetime.strptime(start_date.split("T")[0], '%Y-%m-%d')
                end_date_dt = datetime.strptime(end_date.split("T")[0], '%Y-%m-%d')

        df = get_table_data_detalle(year_filter, start_date_dt, end_date_dt)

    if df.empty:
        return html.Div([html.H3("No se encontraron datos.")])

    data = df.to_dict('records')

    columns = [
        {'name': 'ID', 'id': 'id'},
        {
            'name': 'Movimiento ID',
            'id': 'movimiento_id',
            'presentation': 'markdown'
        },
        {'name': 'Canal', 'id': 'canal'},
        {'name': 'Unidades', 'id': 'unidades'},
        {'name': 'PML', 'id': 'pml'},
        {'name': 'PMF', 'id': 'pmf'},
        {'name': 'PVL', 'id': 'pvl'},
        {'name': 'PVP', 'id': 'pvp'},
        {'name': 'Fecha Movimiento', 'id': 'fecha_movimiento'}
    ]

    for row in data:
        mid = row['movimiento_id']
        row['movimiento_id'] = f"[{mid}](#mov_{mid})"

    tooltip_data = [{'movimiento_id': 'Haz clic para ver detalle'} for _ in data]

    return dash_table.DataTable(
        id='detalle-movimiento-table',
        columns=columns,
        data=data,
        style_table={'height': '300px', 'overflowY': 'auto'},
        style_cell={
            'textAlign': 'center',
            'padding': '5px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px'
        },
        filter_action="native",
        sort_action="native",
        page_size=4,
        tooltip_data=tooltip_data,
        tooltip_duration=None,
        markdown_options={"link_target": "_self"}
    )

@app.callback(
    Output('detalle-movimiento', 'children'),
    Input('detalle-movimiento-table', 'active_cell'),
    State('detalle-movimiento-table', 'data')
)
def mostrar_detalle_movimiento(active_cell, rows):
    if active_cell and active_cell['column_id'] == 'movimiento_id':
        row = rows[active_cell['row']]
        import re
        match = re.match(r'\[(\d+)\]', row['movimiento_id'])
        if match:
            mid = int(match.group(1))
            detalle = get_movimiento_detail(mid)
            if detalle:
                columns = [{'name': k, 'id': k} for k in detalle.keys()]
                data = [detalle]
                return dash_table.DataTable(
                    columns=columns,
                    data=data,
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'center',
                        'fontFamily': 'Arial, sans-serif',
                        'fontSize': '14px',
                        'padding': '5px'
                    },
                    page_size=1,
                    style_header={'fontWeight': 'bold'}
                )
            else:
                return html.Div("No se encontró detalle para el movimiento.")
    return ""

if __name__ == '__main__':
    app.run(debug=True)
