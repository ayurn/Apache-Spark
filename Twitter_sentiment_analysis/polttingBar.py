import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.animation import FuncAnimation
from matplotlib import style 

# plt.style.available
plt.style.use('ggplot')

def animate(i):
    data = pd.read_csv('/home/ayur/HadoopProgram/PySpark Programs/Twitter_sentiment_analysis/data.csv')
    x1 = data['pos']
    x2 = data['neg']
    y1 = data['pos_count']
    y2 = data['neg_count']
    
    plt.cla()
    
    plt.bar(x1,y1,label='pos')
    plt.bar(x2,y2,label='neg')
    
    plt.xlabel("sentiments")
    plt.ylabel("count")
    plt.legend(loc='upper right')
    plt.tight_layout()
    

ani = FuncAnimation(plt.gcf(), animate)

plt.tight_layout()
plt.show()


