# Datasets
The datasets can be found in the input folder, or alternatively online:
- [Impact of AI on Digital Media (2020-2025)](https://www.kaggle.com/datasets/atharvasoundankar/impact-of-ai-on-digital-media-2020-2025)
- [Global Energy Consumption (2000-2024)](https://www.kaggle.com/datasets/atharvasoundankar/global-energy-consumption-2000-2024/data)

# Pipeline
To execute the pipeline, the requirements (pandas, xlswriter) need to be installed (e.g. in a virtual environment):
```cmd
pip install -r requirements.txt
python pipeline.py
```

The output star schema will be stored under ./output/ai_energy_data.xlsx