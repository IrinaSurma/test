import multiprocessing
import os
import random
import shutil
import string
import xml.etree.ElementTree as ET
from zipfile import ZipFile

ARCHIVES_COUNT = 50
FILES_COUNT = 100
NUMBER_OF_PROCESSES = multiprocessing.cpu_count()
TEMPLATE_ROOT = string.Template("""<root>
<var name='id' value='$random_uniq_str'/>
<var name='level' value='$random_num_from_1_to_100'/>
<objects>
$objects
</objects>
</root>
""")
TMP_DIR = "tmp_dir"


def create_data():
    """
    1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:
        <root>
        <var name=’id’ value=’<случайное уникальное строковое значение>’/>
        <var name=’level’ value=’<случайное число от 1 до 100>’/>
        <objects>
        <object name=’<случайное строковое значение>’/>
        <object name=’<случайное строковое значение>’/>
        …
        </objects>
        </root>
        В тэге objects случайное число (от 1 до 10) вложенных тэгов object.
    """
    if os.path.exists(TMP_DIR):
        shutil.rmtree(TMP_DIR)
    os.mkdir(TMP_DIR)
    os.chdir(TMP_DIR)

    for archive in range(ARCHIVES_COUNT):
        with ZipFile(f"{archive}.zip", "w") as file_zip:
            for i_file in range(FILES_COUNT):
                with open(f"{archive}_{i_file}.xml", "w") as file_xml:
                    file_xml.write(TEMPLATE_ROOT.substitute(
                        random_uniq_str=f"{archive}{random.choice(string.ascii_letters)}{i_file}",
                        random_num_from_1_to_100=f"{random.choice(range(1, 101))}",
                        objects="\n".join([f"""<object name='{random.choice(string.ascii_letters)}'/>"""
                                           for _ in range(0, random.choice(range(1, 11)))]),
                    ))
                file_zip.write(f"{archive}_{i_file}.xml")
                os.remove(f"{archive}_{i_file}.xml")
    os.chdir("../")


def worker(input, output):
    for archive in iter(input.get, "STOP"):
        result = [[], []]
        with ZipFile(archive, "r") as z_file:
            work_dir = f"{archive.split('.')[0]}"
            z_file.extractall(work_dir)
            for xml_file in os.listdir(work_dir):
                tree = ET.parse(f"{work_dir}/{xml_file}")
                root = tree.getroot()
                result[0].append(root[0].attrib["value"] + " " + root[1].attrib["value"])
                result[1].append(
                    (root[0].attrib["value"], [root[2][one].attrib["name"] for one in range(len(root[2]))])
                )
            shutil.rmtree(work_dir)
        output.put(result)


def process_files():
    """
    Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
    Первый: id, level - по одной строке на каждый xml файл
    Второй: id, object_name - по отдельной строке для каждого тэга object
    (получится от 1 до 10 строк на каждый xml файл)
    Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора.
    """
    archives = os.listdir(TMP_DIR)
    tasks = multiprocessing.Queue()
    results = multiprocessing.Queue()

    for archive in archives:
        tasks.put(os.path.join(TMP_DIR, archive))

    for i in range(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(tasks, results)).start()

    for i in range(NUMBER_OF_PROCESSES):
        tasks.put("STOP")

    with open("first_part.csv", "w+") as csv_1, open("second_part.csv", "w+") as csv_2:
        for _ in range(len(archives)):
            data = results.get()
            csv_1.write("\n".join([one for one in data[0]]))
            for one in data[1]:
                csv_2.write("\n".join([one[0] + " " + obj for obj in one[1]]))
                csv_2.write("\n")
            csv_1.write("\n")


if __name__ == "__main__":
    create_data()
    process_files()
