import pandas as pd
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')
import re
# matplotlib.rcParams['ps.useafm'] = True
# matplotlib.rcParams['pdf.use14corefonts'] = True
# matplotlib.rcParams['text.usetex'] = True
plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Helvetica, Avant Garde, Computer Modern Sans serif' # Choose a nice font here
plt.rcParams['axes.facecolor']='w'
plt.rcParams['axes.edgecolor'] = 'black'
plt.rcParams['axes.labelcolor'] = 'black'
plt.rcParams['xtick.color']='black'
plt.rcParams['ytick.color']='black'
plt.rcParams['grid.color'] = 'grey'
plt.rcParams['axes.grid.axis'] = 'x'
plt.rcParams['grid.linestyle'] = 'dotted'
plt.rcParams['grid.alpha'] = 0.6


axis_label = {'runtime': 'Time (sec)',
'avg_runtime': 'Time (sec)',
'ndcgscore': 'NDCG',
'avg_ndcg': 'NDCG',
'recall': 'Recall'}



def plot_running_time_against_db_offline(ds_name, col1, col2):

    fname = 'Experiments_results_Sample_'+ds_name + '.csv'
    df = pd.read_csv(fname, header=0)  
    print(df)
    marker_dict = {1: 'o', 2: 's', 3: '^'}
    marker_dict2 = {1: 'x', 2: '|', 3: '*'}

    fig = plt.figure()
    ax3 = fig.add_subplot(111)
    # a = df4.plot(x='db_size', y=['raw', 'prov-all', 'prov-sel', 'solver-sel-SAT*128', 'solver-sel-SMT', 'solver-all-SMT'],
    #     ax=ax3, rot=0, fontsize=20, marker='s', legend=True, logx=True)
    for key in [1,2,3]:
        df[df.edge==key].plot(x='samplerate', y=col1,
            ax=ax3, rot=0, fontsize=20, ms=9, marker=marker_dict[key], 
            legend=True, logy=False, label='Runtime \#edges='+str(key))
    # df.plot(ax=ax3, rot=0, fontsize=20, ms=9, legend=True, logy=True)
    ax3.margins(0.1, None)
    # a.autoscale(tight=False)
    # ax3.set_xticklabels(['0.05', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
    # ax3.set_xticks([0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
    # ax3.set_xticks([1000,4000,10000,40000,100000])
    # ax3.set_yticks([10, 50, 100, 200, 300, 400])
    # ax3.set_yticklabels(['10', '50', '100', '200', '300', ''])
    ax3.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

    ax3.set_xlabel('Sample rate', fontsize=22)  
    ax3.set_ylabel(axis_label[col1], fontsize=22)


    ax4 = ax3.twinx()  # instantiate a second axes that shares the same x-axis

    # color = 'tab:blue'
    # ax4.set_ylabel('sin', color=color)  # we already handled the x-label with ax1
    for key in [1,2,3]:
        df[df.edge==key].plot(x='samplerate', y=col2,
            ax=ax4, rot=0, fontsize=20, ms=9, marker=marker_dict2[key], style='.-.',
            legend=True, label=axis_label[col2] + ' \#edges='+str(key))
    ax4.tick_params(axis='y')
    if col2 == 'ndcgscore':
        if ds_name == 'NBA':
            # ax4.set_yticklabels(['0.6', '0.7', '0.8', '0.9', '1.0'])
            # ax4.set_yticklabels(['0', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
            # ax4.set_yticks([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
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

    # plt.legend(loc=2, fontsize='xx-large')
    ax3.get_legend().remove()
    ax4.get_legend().remove()

    plt.legend(handles, labels, prop={'size': 16}, loc='lower right', borderpad=0.5,#labelspacing=0,
        handlelength=1, fancybox=True, framealpha=0.5)

    # plt.xlabel('sample rate', fontsize=22)
    plt.savefig(ds_name + '_'+ col1+'_against_' + col2 + '.pdf', bbox_inches='tight', format='pdf')
    plt.close()


def plot_running_time_against_db_offline_all(ds_name_list, col1, col2):

    def plot_subplot(fig, ds_name, ax3):
        fname = 'Experiments_results_Sample_'+ds_name + '_avg.csv'
        df = pd.read_csv(fname, header=0)  
        # ax3 = fig.add_subplot(111)
        # a = df4.plot(x='db_size', y=['raw', 'prov-all', 'prov-sel', 'solver-sel-SAT*128', 'solver-sel-SMT', 'solver-all-SMT'],
        #     ax=ax3, rot=0, fontsize=20, marker='s', legend=True, logx=True)
        # for key in [1,2,3]:
        for key in [3]:
            df[df.edge==key].plot(x='samplerate', y=col1,
                ax=ax3, rot=0, fontsize=24, ms=14, marker=marker_dict[key], 
                legend=True, logy=False, label='Runtime \#edges='+str(key))
        # df.plot(ax=ax3, rot=0, fontsize=20, ms=9, legend=True, logy=True)
        ax3.margins(0.1, None)
        # a.autoscale(tight=False)
        # ax3.set_xticklabels(['0.05', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
        # ax3.set_xticks([0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
        # ax3.set_xticks([1000,4000,10000,40000,100000])
        # ax3.set_yticks([10, 50, 100, 200, 300, 400])
        # ax3.set_yticklabels(['10', '50', '100', '200', '300', ''])
        if ds_name == 'MIMIC':               
            ax3.set_yticklabels(['0', '100', '200', '300', '400', ''])
            ax3.set_yticks([0, 100, 200, 300, 400, 500])
        else:
            ax3.set_yticklabels(['0', '100', '200', '300', '400', ''])
            ax3.set_yticks([0, 100, 200, 300, 400, 500])

        ax3.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

        ax3.set_xlabel('Sample rate', fontsize=26)  
        ax3.set_ylabel(axis_label[col1], fontsize=26)


        ax4 = ax3.twinx()  # instantiate a second axes that shares the same x-axis

        # color = 'tab:blue'
        # ax4.set_ylabel('sin', color=color)  # we already handled the x-label with ax1
        # for key in [1,2,3]:
        for key in [3]:
            df[df.edge==key].plot(x='samplerate', y=col2,
                ax=ax4, rot=0, fontsize=24, ms=14, marker=marker_dict2[key], style='.-.',
                legend=True, label=axis_label[col2] + ' \#edges='+str(key),
                color='steelblue' if col2.find('ndcg') > -1 else 'red')
        ax4.tick_params(axis='y')
        if col2.find('ndcg') > -1:
            if ds_name == 'NBA':
                # ax4.set_yticklabels(['0.6', '0.7', '0.8', '0.9', '1.0'])
                # ax4.set_yticklabels(['0', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
                # ax4.set_yticks([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
                # ax4.set_yticklabels(['0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
                # ax4.set_yticks([0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
                # ax4.set_yticklabels(['0.5', '0.6', '0.7', '0.8', '0.9', '1.0'])
                # ax4.set_yticks([0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
                ax4.set_yticklabels(['0.6', '0.7', '0.8', '0.9', '1.0'])
                ax4.set_yticks([0.6, 0.7, 0.8, 0.9, 1.0])
            else:
                ax4.set_yticklabels(['0.6', '0.7', '0.8', '0.9', '1.0'])
                ax4.set_yticks([0.6, 0.7, 0.8, 0.9, 1.0])
                # ax4.set_yticklabels(['', '0.8', '', '0.9', '', '1.0'])
                # ax4.set_yticks([0.75, 0.8, 0.85, 0.9, 0.95, 1.0])
            
            
                
        ax4.set_xticklabels(['0.05', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
        ax4.set_xticks([0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])    
        ax4.set_ylabel(axis_label[col2], fontsize=26)  # we already handled the x-label with ax1
        


        handles,labels = [None for i in range(6)],[None for i in range(6)]

        cnt = 0
        len_cnt = 0
        for ax in fig.axes:
            cnt += 1
            if cnt % 2 == 1:
                continue
            for h,l in zip(*ax.get_legend_handles_labels()):
                print(h)
                print(l)
                print(cnt)
                # handles.append(h)
                # labels.append(l)
                if len_cnt < 3:
                    handles[len_cnt * 2] = h
                    labels[len_cnt * 2] = l
                else:
                    handles[(len_cnt-3) * 2 + 1] = h
                    labels[(len_cnt-3) *2 + 1] = l
                len_cnt += 1

        # plt.legend(loc=2, fontsize='xx-large')
        ax3.get_legend().remove()
        ax4.get_legend().remove()
        return handles, labels

    
    
    marker_dict = {1: 'o', 2: 's', 3: '^'}
    marker_dict2 = {1: 'x', 2: '|', 3: '*'}

    # fig = plt.figure()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15,5))
    # fig.tight_layout()

    plot_subplot(fig, ds_name_list[0], ax1)
    handles, labels = plot_subplot(fig, ds_name_list[1], ax2)
    print(handles)
    print(labels)
    # plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)
    # plt.legend(handles, labels, prop={'size': 20}, loc='lower center', borderpad=0.5,#labelspacing=0,
    #     handlelength=1, fancybox=True, framealpha=0.5)
    # label = plt.ylabel("y-label")
    # label.set_color("black")
    fig.legend(handles, labels, prop={'size': 20}, loc='upper center', borderpad=0.5,#labelspacing=0,
        handlelength=1, fancybox=True, framealpha=0.0, ncol=3, bbox_to_anchor=(0.5, 1.17))
    plt.tight_layout()

    # plt.xlabel('sample rate', fontsize=22)
    # plt.savefig('all_'+ col1+'_against_' + col2 + '.pdf', bbox_inches='tight', format='pdf')
    plt.savefig('jg3_all_'+ col1+'_against_' + col2 + '.pdf', bbox_inches='tight', format='pdf')
    plt.close()


def plot_running_time_number_jg_bar(two_y_axis=False):

    # runtime = [596.1403151,59.20030594,84.47665596,508.7447991,36.57828403,243.8028731,695.570328,255.9841588,138.7882333,132.3358893]
    # jgnum = [9,4,7,9,8,17,21,17,17,11]
    # ptsize = [140911,23193,20916,103711,3205,157,164,155,114,132]
    # index=['mimic1','mimic2', 'mimic3','mimic4','mimic5', 'nba1','nba2','nba3','nba4','nba5']
    # runtime = [243.8028731,695.570328,255.9841588,138.7882333,132.3358893, 
    #     596.1403151,59.20030594,84.47665596,508.7447991,36.57828403]
    # jgnum = [17,21,17,17,11, 9,4,7,9,8]
    runtime = [269.5466475,717.2851119,276.6834949,125.0244987,263.3097157,
        640.2224851,64.56710558,129.6306346,574.4649469,71.77737279]
    runtime_std = [4.819265321,27.92007554,1.388157182,1.128634984,1.302121983,
        21.59339315,0.6579186283,10.45846803,12.48934078,1.356538856]
    jgnum = [20,21,20,17,20,9,4,8,9,9]
    ptsize = [157,164,155,114,132, 140911,23193,20916,103711,3205]
    index=['$Q_{w' + str(i) + '}$' for i in range(1, 11)]
    df = pd.DataFrame({'Runtime (sec)': runtime,
                   '\# Join Graph': jgnum}, index=index)
    df_err = pd.DataFrame({'Runtime (sec)': runtime_std,
                   '\# Join Graph': [0 for i in range(10)]}, index=index)
    fig = plt.figure()
    if two_y_axis:
        ax = df.plot.bar(secondary_y= '\# Join Graph' , rot=0, mark_right=False, width=0.75)
        # fig, ax = plt.subplots()
        ax1, ax2 = plt.gcf().get_axes() # gets the current figure and then the axes
        ax1.tick_params(axis="y", labelsize=18)
        ax1.set_yticks([0,200,400,600])    
        ax1.set_ylabel('Runtime (sec)', fontsize=18)
        ax2.tick_params(axis="y", labelsize=18)
        ax2.set_yticks([0,5,10,15,20])    
        ax2.set_ylabel('\# Join Graph', fontsize=18) 
        # plt.legend(loc=2, fontsize = 'x-large')
    
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax.right_ax.get_legend_handles_labels()
        handles = h1+h2
        labels = l1+l2
        ax.get_legend().remove()
        # ax.legend(handles, labels, loc='upper left', ncol=3,
        #     bbox_to_anchor=(0.25, -.575))
        plt.legend(handles, labels, prop={'size': 16}, loc='upper left', borderpad=0.5,#labelspacing=0,
            handlelength=1, fancybox=True, framealpha=0.5)

    else:
        ax = fig.add_subplot(111)
        ax = df.plot.bar(rot=0,subplots=True, title=['', ''], yerr=df_err, error_kw=dict(lw=6, capsize=0, capthick=2))
        ax[0].legend(loc=0)  
        ax[1].legend(loc=0)  
        # ax[1].tick_params(axis='y', which='minor', labelsize=22)
        # ax[1].set_yticks(fontsize=16)
        # ax[0].tick_params(axis="x", labelsize=18)
        ax[0].tick_params(axis="y", labelsize=18)
        ax[0].set_yticks([0,200,400,600])    
        ax[0].set_ylabel('Runtime (sec)', fontsize=18)
        ax[1].tick_params(axis="x", labelsize=14)
        ax[1].tick_params(axis="y", labelsize=18)
        ax[1].set_yticks([0,5,10,15,20])    
        ax[1].set_ylabel('\# Join Graph', fontsize=18)  # we already handled the x-label with ax1



    # ax[1].set_xticklabels(['0.05', '', '0.2', '', '0.4', '', '0.6', '', '0.8', '', '1.0'])
    # ax[1].set_xticks([0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])    
    
    plt.savefig('workload_runtime_jgnum.pdf', bbox_inches='tight', format='pdf')
    plt.close()    

# plot_running_time_against_db_offline('MIMIC', 'runtime', 'ndcgscore')
# plot_running_time_against_db_offline('NBA', 'runtime', 'ndcgscore')

# plot_running_time_against_db_offline_all(['NBA','MIMIC'], 'runtime', 'ndcgscore')
# plot_running_time_against_db_offline_all(['NBA','MIMIC'], 'runtime', 'recall')
plot_running_time_against_db_offline_all(['NBA','MIMIC'], 'avg_runtime', 'avg_ndcg')

# plot_running_time_against_db_offline('MIMIC', 'runtime', 'recall')
# plot_running_time_against_db_offline('NBA', 'runtime', 'recall')

# plot_running_time_number_jg_bar(two_y_axis=True)
# plot_running_time_number_jg_bar()

