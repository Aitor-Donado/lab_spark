# %%
archivo = "pyspark"

# %%
import os
esta_carpeta = os.getcwd()

# %%
import json

# %% [markdown]
# Apertura del archivo Markdown

# %%
with open(os.path.join(esta_carpeta, archivo + ".md"), "r") as f:
    markdown_text = f.read()
    markdown_celdas = markdown_text.split("```")

# %% [markdown]
# Creación de los dos tipos de celdas

# %%
celda_md = {"cell_type": 'markdown', "source": [], "metadata": {"id": ""}}
celda_cod = {"cell_type": 'code', "execution_count": None, "metadata": {"id": ""}, "outputs": [], "source": []}

# %%
cuaderno = {'nbformat': 4, 'nbformat_minor': 0, 'metadata': {'colab': {'provenance': [], 'authorship_tag': 'ABX9TyMwI+o5PNIzQHlo0pPdQ3ZS'}, 'kernelspec': {'name': 'python3', 'display_name': 'Python 3'}, 'language_info': {'name': 'python'}}, 'cells': []}

# %%
for i, texto in enumerate(markdown_celdas):
  if texto.startswith("python") or texto.startswith("r"):
    celda_a_anadir = celda_cod.copy()
    celda_a_anadir['source'] = "\n".join([linea for linea in markdown_celdas[i].split("\n") if len(linea) > 0][1:])
    # celda_a_anadir['source'] = [linea + "\n" for linea in markdown_celdas[i].split("\n") if len(linea) > 0][1:]
  else:
    celda_a_anadir = celda_md.copy()
    celda_a_anadir['source'] = [linea + "\n" for linea in markdown_celdas[i].split("\n")]
  cuaderno["cells"].append(celda_a_anadir)

# %%
cuaderno_text = json.dumps(cuaderno)

# %%
with open(os.path.join(esta_carpeta, archivo + ".ipynb"), "w") as f:
    f.write(cuaderno_text)


