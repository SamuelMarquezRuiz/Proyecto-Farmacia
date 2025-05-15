import os
import pandas as pd
import sqlite3

def insertar_datos_desde_excel(ruta_excel, ruta_db):
    try:
        # === Leer Excel ===
        df = pd.read_excel(ruta_excel)
        df.columns = df.columns.str.strip().str.lower()
        print("Columnas en el Excel:", list(df.columns))
        print("¿Hay valores nulos en 'gfh'?:", df['gfh'].isnull().any())

        # === Conectar / Crear base de datos ===
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()

        # === Crear tablas si no existen ===
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

        # === Insertar en unidad ===
        unidades = df[['tipo', 'denominacion', 'gfh']].drop_duplicates()
        cursor.executemany("""
            INSERT OR IGNORE INTO unidad (tipo, denominacion, gfh)
            VALUES (?, ?, ?)
        """, unidades.itertuples(index=False, name=None))
        conn.commit()

        # === Insertar en producto ===
        productos = df[['espec', 'registrado']].drop_duplicates()
        cursor.executemany("""
            INSERT OR IGNORE INTO producto (espec, nombre)
            VALUES (?, ?)
        """, productos.itertuples(index=False, name=None))
        conn.commit()

        # === Insertar en movimiento ===
        movimientos = df[['gfh', 'espec', 'tipo_e_s']].copy()
        print("Insertando movimientos con GFH:", movimientos['gfh'].unique())
        cursor.executemany("""
            INSERT INTO movimiento (unidad_gfh, producto_espec, tipo_e_s)
            VALUES (?, ?, ?)
        """, movimientos.itertuples(index=False, name=None))
        conn.commit()

        # === Obtener IDs insertados ===
        num_rows = len(movimientos)
        rows = cursor.execute("SELECT id FROM movimiento ORDER BY id DESC LIMIT ?", (num_rows,)).fetchall()
        movimiento_ids = [r[0] for r in reversed(rows)]

        # Convertir datos de formato ancho a largo
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

        # Combinar
        detalles_final = pd.concat(detalle_data, ignore_index=True)

        # Insertar en detalle_movimiento
        cursor.executemany("""
            INSERT INTO detalle_movimiento (
                movimiento_id, canal, unidades, pml, pmf, pvl, pvp, pvf
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, detalles_final.itertuples(index=False, name=None))
        conn.commit()

        # === Cerrar conexión ===
        conn.close()
        print("✅ Inserción completada correctamente.")

    except Exception as e:
        print(f"❌ Error durante la inserción: {e}")

# === Parámetros ===
ruta_excel = './Data/20250506_datos_proyecto_farmacia.xlsx'     # Ajusta la ruta si tu Excel está en otro sitio
ruta_db = os.path.join(os.path.dirname(__file__), "datos.db")    # Base de datos SQLite local

insertar_datos_desde_excel(ruta_excel, ruta_db)
