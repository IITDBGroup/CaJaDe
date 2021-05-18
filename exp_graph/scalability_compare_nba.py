import pandas as pd
import matplotlib
matplotlib.use('PDF')
import pylab as pl
from numpy import arange,power

def main():
    df=pd.read_csv('scalability_nba.csv')

    pl.ioff()
    
    ax=df.plot.bar(x='db_size', y='total', rot=0, color='dodgerblue')

    legend = ax.legend(bbox_to_anchor=(0.005, 1.04),prop={'size': 20},labels=['running time'],loc=2,
              borderpad=0.1,labelspacing=0,handlelength=1,handletextpad=0.2,
              columnspacing=0.5,framealpha=1)
    legend.get_frame().set_edgecolor('black')
    
    # axis labels and tics
    ax.set_ylabel('time (sec)', fontsize=20)
    ax.set_xlabel('NBA DB (#row 1.0=1.4 Million)', fontsize=20)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    pl.xticks(rotation=0)    

    # grid
    ax.yaxis.grid(which='major',linewidth=3.0,linestyle=':')
    ax.set_axisbelow(True)

    
    pl.show()
    pl.savefig("scalability_nba.pdf", bbox_inches='tight')
    
if __name__=="__main__":
    main()
