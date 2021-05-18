import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('PDF')
import pylab as pl
from numpy import arange,power
import matplotlib.patches as mpatches


def main():
    df=pd.read_csv('april_4_Scalability_nba.csv')


    df = df.sort_values(['size', 'f1_sample_rate'], ascending=[True, True])

    df = df[['size','f1_sample_rate', 'totals']]

    df_07 = df[df['f1_sample_rate']==0.7]

    ratio_df = df[(df['f1_sample_rate'] == 0.7) & (df['size'] == 0.5)]
    slope_df = ratio_df['totals'] / ratio_df['size']
    slope = slope_df.iloc[0].item()
    print(f"slope is {str(slope)}")

    df_07['expected_runtime'] = df_07['size'] * slope

    print(df_07)

    # ax = df_07[['size', 'expected_runtime']].plot(
    #     x='size', linestyle='-', marker='o')

    # df.plot(x='size', kind='bar',ax=ax, rot=0)

    # df_07['ideal'] = df_07['totals']*slope
    
    df = df.sort_values(['size', 'f1_sample_rate'], ascending=[True, True])
    df['size'] = df['size'].astype('str')
    pl.ioff()
    df = df.pivot(index='size', columns='f1_sample_rate', values='totals')
    print(df)

    ax=df.plot.bar(rot=0, width=0.8)
    df_07.reset_index()["expected_runtime"].plot(kind="line", style='-o',ax=ax, label='linear scaling', color='black')

    # df_07.plot(x='size', y='expected_runtime', style='-o', ax=ax)
    # print(df_07)


    # print(ratio)

    handles = [mpatches.Patch(color='none')]

    auto_handles, auto_labels = ax.get_legend_handles_labels()

    print(auto_handles)
    print(auto_labels)

    handles.extend(auto_handles)
    # scale_handle.append(auto_handles[0])

    legend1 = ax.legend(bbox_to_anchor=(0.005, 1.04),prop={'size': 15},loc=2,
              borderpad=0.1,labelspacing=0,handlelength=1,handletextpad=0.2,
              columnspacing=0.1,framealpha=1, handles=handles, labels=['', 'linear scaling', '0.1 f1 sample', '0.3 f1 sample', '0.5 f1 sample', '0.7 f1 sample'],fontsize=5)

    print(legend1.get_texts())

    legend1.get_frame().set_edgecolor('black')
    # legend2.get_frame().set_edgecolor('black')

    
    # axis labels and tics
    ax.set_ylabel('time (sec)', fontsize=20)
    ax.set_xlabel('NBA DB (#row 1.0=1.4 Million)', fontsize=20)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(20) 

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(20) 
    
    pl.show()
    pl.savefig("scalability_nba_with_line_new.pdf", bbox_inches='tight')
    
if __name__=="__main__":
    main()
