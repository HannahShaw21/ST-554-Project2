{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "254f0c7d-97e1-4de3-85e8-be6777856b6b",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import DataFrame\n",
    "from pyspark.sql import functions as F\n",
    "from functools import reduce\n",
    "from pyspark.sql.types import *\n",
    "import pandas as pd\n",
    "import importlib"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "402baab9-9c5f-4d21-8410-a2dae5cc71dd",
   "metadata": {},
   "outputs": [
    {
     "ename": "NameError",
     "evalue": "name 'df' is not defined",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mNameError\u001b[0m                                 Traceback (most recent call last)",
      "Cell \u001b[0;32mIn[2], line 1\u001b[0m\n\u001b[0;32m----> 1\u001b[0m \u001b[38;5;28;01mclass\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[38;5;21;01mSparkDataCheck\u001b[39;00m:\n\u001b[1;32m      2\u001b[0m     \u001b[38;5;28;01mdef\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[38;5;21m__init__\u001b[39m(\u001b[38;5;28mself\u001b[39m, DataFrame \u001b[38;5;241m=\u001b[39m df):\n\u001b[1;32m      3\u001b[0m         \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39mdf \u001b[38;5;241m=\u001b[39m DataFrame\n",
      "Cell \u001b[0;32mIn[2], line 2\u001b[0m, in \u001b[0;36mSparkDataCheck\u001b[0;34m()\u001b[0m\n\u001b[1;32m      1\u001b[0m \u001b[38;5;28;01mclass\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[38;5;21;01mSparkDataCheck\u001b[39;00m:\n\u001b[0;32m----> 2\u001b[0m     \u001b[38;5;28;01mdef\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[38;5;21m__init__\u001b[39m(\u001b[38;5;28mself\u001b[39m, DataFrame \u001b[38;5;241m=\u001b[39m \u001b[43mdf\u001b[49m):\n\u001b[1;32m      3\u001b[0m         \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39mdf \u001b[38;5;241m=\u001b[39m DataFrame\n\u001b[1;32m      5\u001b[0m     \u001b[38;5;129m@classmethod\u001b[39m\n\u001b[1;32m      6\u001b[0m     \u001b[38;5;28;01mdef\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[38;5;21minstance_readcsv\u001b[39m(\u001b[38;5;28mself\u001b[39m): \u001b[38;5;66;03m#creates an instance while reading in a csv file\u001b[39;00m\n",
      "\u001b[0;31mNameError\u001b[0m: name 'df' is not defined"
     ]
    }
   ],
   "source": [
    "class SparkDataCheck:\n",
    "    def __init__(self, DataFrame = df):\n",
    "        self.df = DataFrame\n",
    "        \n",
    "    @classmethod\n",
    "    def instance_readcsv(self): #creates an instance while reading in a csv file\n",
    "        spark.read.load()\n",
    "        return\n",
    "        \n",
    "    @classmethod\n",
    "    def instance_dataframe(self): #creates an instance from a pandas dataframe\n",
    "        spark.CreateDataFrame()\n",
    "        return"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c211104e-b0ea-4803-ab42-e901717246e3",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "pySpark3",
   "language": "python",
   "name": "pyspark3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.15"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
