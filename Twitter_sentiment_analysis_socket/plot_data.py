'''
@Author: Ayur Ninawe
@Date: 2021-08-22
@Last Modified by: Ayur Ninawe
@Last Modified time: 2021-08-23
@Title : Program to plot line chart for live data by reading info.
'''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.animation import FuncAnimation
from matplotlib import style 

# plt.style.available
style.use("seaborn")

def animate(i):
    data = pd.read_csv('/home/ayur/HadoopProgram/PySpark Programs/Twitter_sentiment_analysis_socket/tweet_data.csv')
    y1 = data['pos']
    y2 = data['neg']
    x1 = data['tweet_count']
    plt.cla()
    plt.plot(x1,y1,label="positive")
    plt.plot(x1,y2,label='negative')
    plt.xlabel("sentiments")
    plt.ylabel("count")
    plt.legend(loc='upper left')
    plt.tight_layout()
   
    # for x1, value in enumerate(y1):
    #     plt.text(value, x1, str(value))

ani = FuncAnimation(plt.gcf(), animate)

plt.tight_layout()
plt.show()