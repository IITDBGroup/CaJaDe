import pandas as pd
import matplotlib
matplotlib.use('PDF')
import pylab as pl
from numpy import arange,power
import matplotlib.patches as mpatches


def main():
    df=pd.read_csv('March1st_Scalability_NBA.csv')

    df = df[['size','f1_sample_rate', 'totals']]
    df = df.sort_values(['size', 'f1_sample_rate'], ascending=[True, True])
    df['size'] = df['size'].astype('str')
    pl.ioff()
    df = df.pivot(index='size', columns='f1_sample_rate', values='totals')

    ax=df.plot.bar(rot=0, width=0.8)

    handles = [mpatches.Patch(color='none', label='F1 sample rate')]

    auto_handles, auto_labels = ax.get_legend_handles_labels()

    handles.extend(auto_handles)

    legend = ax.legend(bbox_to_anchor=(0.005, 1.04),prop={'size': 15},loc=2,
              borderpad=0.1,labelspacing=0,handlelength=1,handletextpad=0.2,
              columnspacing=0.5,framealpha=1, handles=handles, fontsize=5)

    legend.get_frame().set_edgecolor('black')
    
    # axis labels and tics
    ax.set_ylabel('time (sec)', fontsize=20)
    ax.set_xlabel('NBA DB (#row 1.0=1.4 Million)', fontsize=20)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    # pl.xticks(rotation=0)    

    # grid
    # ax.yaxis.grid(which='major',linewidth=2.0,linestyle=':')
    # ax.set_axisbelow(True)

    
    pl.show()
    pl.savefig("scalability_nba.pdf", bbox_inches='tight')
    
if __name__=="__main__":
    main()
