"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby
import shutil


def copy_raw_files_to_input_folder(n):
    """Genera n copias de los archivos en files/raw a files/input"""
    # Crear directorio input si no existe
    os.makedirs("files/input", exist_ok=True)
    
    # Limpiar directorio input si ya tenía archivos
    for filename in glob.glob("files/input/*.txt"):
        os.remove(filename)
    
    # Obtener lista de archivos en raw
    raw_files = glob.glob("files/raw/*.txt")
    
    # Generar las copias
    for i in range(1, n + 1):
        for raw_file in raw_files:
            filename = os.path.basename(raw_file)
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_{i}{ext}"
            shutil.copy(raw_file, f"files/input/{new_filename}")


def load_input(input_directory):
    """Carga todos los archivos de un directorio y retorna lista de tuplas (nombre, línea)"""
    lines = []
    for filename in glob.glob(f"{input_directory}/*.txt"):
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                if line.strip():  # Ignorar líneas vacías
                    lines.append((os.path.basename(filename), line.strip()))
    return lines


def line_preprocessing(sequence):
    """Preprocesamiento de líneas de texto"""
    processed = []
    for filename, line in sequence:
        # Convertir a minúsculas y eliminar puntuación
        cleaned_line = line.lower()
        for char in '.,;:!?"\'()[]{}':
            cleaned_line = cleaned_line.replace(char, ' ')
        words = cleaned_line.split()
        processed.extend([(filename, word) for word in words])
    return processed


def mapper(sequence):
    """Convierte cada palabra en una tupla (palabra, 1)"""
    return [(word, 1) for _, word in sequence]


def shuffle_and_sort(sequence):
    """Ordena las tuplas por la clave (palabra)"""
    return sorted(sequence, key=lambda x: x[0])


def reducer(sequence):
    """Reduce las tuplas sumando los valores para cada clave"""
    reduced = []
    for key, group in groupby(sequence, lambda x: x[0]):
        total = sum(value for _, value in group)
        reduced.append((key, total))
    return reduced


def create_ouptput_directory(output_directory):
    """Crea un directorio de salida, borrándolo primero si existe"""
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)


def save_output(output_directory, sequence):
    """Guarda el resultado en un archivo part-00000"""
    with open(f"{output_directory}/part-00000", 'w', encoding='utf-8') as file:
        for key, value in sequence:
            file.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Crea un archivo _SUCCESS en el directorio de salida"""
    with open(f"{output_directory}/_SUCCESS", 'w', encoding='utf-8') as file:
        file.write("")


def run_job(input_directory, output_directory):
    """Orquesta todo el proceso de MapReduce"""
    # 1. Cargar datos de entrada
    lines = load_input(input_directory)
    
    # 2. Preprocesamiento
    preprocessed = line_preprocessing(lines)
    
    # 3. Mapeo
    mapped = mapper(preprocessed)
    
    # 4. Shuffle and Sort
    shuffled_sorted = shuffle_and_sort(mapped)
    
    # 5. Reducción
    reduced = reducer(shuffled_sorted)
    
    # 6. Crear directorio de salida
    create_ouptput_directory(output_directory)
    
    # 7. Guardar resultados
    save_output(output_directory, reduced)
    
    # 8. Crear marcador
    create_marker(output_directory)


if __name__ == "__main__":
    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")