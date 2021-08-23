from itertools import count
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation


plt.style.use('fivethirtyeight')

def animate(i):
    data = pd.read_csv('/home/ayur/HadoopProgram/PySpark Programs/Twitter_sentiment_analysis/data.csv')
    y1 = data['pos_count']
    y2 = data['neg_count']
    
    plt.cla()
    plt.plot(y1, label='positive' )
    plt.plot(y2,label='negative')

    plt.legend(loc='upper left')
    plt.tight_layout()

ani = FuncAnimation(plt.gcf(), animate, interval=1000)

plt.tight_layout()
plt.show()
