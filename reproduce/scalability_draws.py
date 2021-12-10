import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('PDF')
import pylab as pl
from numpy import arange,power
import matplotlib.patches as mpatches
import warnings
warnings.filterwarnings('ignore')


def scalability_draw(ds_name, filename):
    
    df=pd.read_csv(f'{filename}.csv')

    df = df.sort_values(['size', 'f1_sample_rate'], ascending=[True, True])

    df = df[['size','f1_sample_rate', 'total']]

    df_07 = df[df['f1_sample_rate']==0.7]

    ratio_df = df[(df['f1_sample_rate'] == 0.7) & (df['size'] == 0.5)]
    slope_df = ratio_df['total'] / ratio_df['size']
    slope = slope_df.iloc[0].item()

    df_07['expected_runtime'] = df_07['size'] * slope
    
    df = df.sort_values(['size', 'f1_sample_rate'], ascending=[True, True])
    df['size'] = df['size'].astype('str')
    pl.ioff()
    df = df.pivot(index='size', columns='f1_sample_rate', values='total')

    ax=df.plot.bar(rot=0, width=0.8)
    df_07.reset_index()["expected_runtime"].plot(kind="line", style='-o',ax=ax, label='linear scaling', color='black')

    handles = [mpatches.Patch(color='none')]

    auto_handles, auto_labels = ax.get_legend_handles_labels()

    handles.extend(auto_handles)
    # scale_handle.append(auto_handles[0])

    legend1 = ax.legend(prop={'size': 20},loc=2,
              borderpad=0.1,labelspacing=0,handlelength=1,handletextpad=0.2,
              columnspacing=0.1,framealpha=1, handles=handles, labels=['', 'linear scaling', '0.1 f1 sample', '0.3 f1 sample', '0.5 f1 sample', '0.7 f1 sample'],fontsize=5)

    legend1.get_frame().set_edgecolor('black')

    
    # axis labels and tics
    ax.set_ylabel('time (sec)', fontsize=20)
    # ax.set_xlabel('MIMIC DB (#row 1.0=1.1 Million)', fontsize=20)
    ax.set_xlabel('NBA DB (#row 1.0=1.4 Million)', fontsize=20)


    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(20) 
    
    pl.savefig(f"{filename}.pdf", bbox_inches='tight')