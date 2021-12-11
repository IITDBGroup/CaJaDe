import pandas as pd
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
import re


axis_label = {'runtime': 'Time (sec)',
'avg_runtime': 'Time (sec)',
'ndcg_score': 'NDCG',
'avg_ndcg': 'NDCG',
'recall': 'Recall'}



def plot_running_time_against_db_offline(ds_name, col1, col2, filename):

    df = pd.read_csv(filename+'.csv', header=0)  
    # print(df)
    marker_dict = {1: 'o', 2: 's', 3: '^'}
    marker_dict2 = {1: 'x', 2: '|', 3: '*'}

    fig = plt.figure()
    ax3 = fig.add_subplot(111)

    for key in [1,2,3]:
        df[df.edge==key].plot(x='sample_rate', y=col1,
            ax=ax3, rot=0, fontsize=20, ms=9, marker=marker_dict[key], 
            legend=True, logy=False, label='Runtime: #edges='+str(key))
    ax3.margins(0.1, None)

    ax3.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

    ax3.set_xlabel('Sample rate', fontsize=22)  
    ax3.set_ylabel(axis_label[col1], fontsize=22)


    ax4 = ax3.twinx()  # instantiate a second axes that shares the same x-axis

    for key in [1,2,3]:
        df[df.edge==key].plot(x='sample_rate', y=col2,
            ax=ax4, rot=0, fontsize=20, ms=9, marker=marker_dict2[key], style='.-.',
            legend=True, label=axis_label[col2] + ': edges='+str(key))
    ax4.tick_params(axis='y')
    if col2 == 'ndcg_score':
        if ds_name == 'NBA':
            ax4.set_yticklabels(['0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
            ax4.set_yticks([0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
        else:
            ax4.set_yticklabels(['0.6', '0.7', '0.8', '0.9', '1.0'])
            ax4.set_yticks([0.6, 0.7, 0.8, 0.9, 1.0])
    ax4.set_xticklabels(['0.05', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
    ax4.set_xticks([0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])    
    ax4.set_ylabel(axis_label[col2], fontsize=22)  # we already handled the x-label with ax1
    


    handles,labels = [],[]
    for ax in fig.axes:
        for h,l in zip(*ax.get_legend_handles_labels()):
            print(h)
            print(l)
            handles.append(h)
            labels.append(l)

    ax3.get_legend().remove()
    ax4.get_legend().remove()

    plt.legend(handles, labels, prop={'size': 12}, loc='upper center', bbox_to_anchor=(0.5, 1.2),
              ncol=3, fancybox=True, shadow=True)

    plt.savefig(filename+'.pdf', bbox_inches='tight', format='pdf')
    plt.close()



def plot_running_time_number_jg_bar_df(filename, two_y_axis=False):

    df=pd.read_csv(f'{filename}.csv')
    df_run_time_agg = df.groupby('query_id').runtime.agg(['mean', 'std'])
    df_num_jgs_agg = df.groupby('query_id').num_jgs.agg(['mean', 'std'])

    index=['$Q_{w' + str(i) + '}$' for i in range(1, 6)]
    df_agg = pd.DataFrame({'Runtime (sec)': list(df_run_time_agg['mean']),
                   '\# Join Graph': list(df_num_jgs_agg['mean'])}, index=index)
    df_err = pd.DataFrame({'Runtime (sec)': list(df_run_time_agg['std']),
                   '\# Join Graph': list(df_num_jgs_agg['std'])}, index=index)
    
    fig = plt.figure()

    if two_y_axis:
        ax = df.plot.bar(secondary_y= '\# Join Graph' , rot=0, mark_right=False, width=0.75)
        ax1, ax2 = plt.gcf().get_axes() # gets the current figure and then the axes
        ax1.tick_params(axis="y", labelsize=18)
        ax1.set_yticks([0,200,400,600])    
        ax1.set_ylabel('Runtime (sec)', fontsize=18)
        ax2.tick_params(axis="y", labelsize=18)
        ax2.set_yticks([0,5,10,15,20])    
        ax2.set_ylabel('\# Join Graph', fontsize=18) 
    
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax.right_ax.get_legend_handles_labels()
        handles = h1+h2
        labels = l1+l2
        ax.get_legend().remove()
        plt.legend(handles, labels, prop={'size': 16}, loc='upper left', borderpad=0.5,#labelspacing=0,
            handlelength=1, fancybox=True, framealpha=0.5)

    else:
        ax = fig.add_subplot(111)
        ax = df_agg.plot.bar(rot=0,subplots=True, title=['', ''], 
            yerr=df_err,
            error_kw=dict(lw=6, capsize=0, capthick=2))
        ax[0].legend(loc=0)  
        ax[1].legend(loc=0) 
        ax[0].tick_params(axis="y", labelsize=18)
        ax[0].set_yticks([0,200,400,600])    
        ax[0].set_ylabel('Runtime (sec)', fontsize=18)
        ax[1].tick_params(axis="x", labelsize=14)
        ax[1].tick_params(axis="y", labelsize=18)
        ax[1].set_yticks([0,5,10,15,20])    
        ax[1].set_ylabel('\# Join Graph', fontsize=18)  # we already handled the x-label with ax1


    plt.savefig(f'{filename}.pdf', bbox_inches='tight', format='pdf')
    plt.close()    
