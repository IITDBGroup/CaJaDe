import pandas as pd
import matplotlib
matplotlib.use('PDF')
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc
import matplotlib.patches as mpatches
from matplotlib.ticker import MaxNLocator


def main():
    # df=pd.read_csv('lca_evals_summary.csv')
    df=pd.read_csv('lca_new.csv')

    fig = plt.figure()
    ax1 = fig.add_subplot(111)

    # DFs
    apt1=df.query('APT==5 and is_ref!=1')
    apt1_gt = df.query('APT==5 and is_ref==1')

    # colors
    col_time='red'
    col_match='blue'
    col_ref = 'black'

    rc('mathtext', default='regular')

    # plot settings
    mymarker=['s','o','v','x']
    msize=80.0
    mymarkerlw=1.0
    mylinewd = 1.0

    # plot lines
    ax2 = ax1.twinx()
    lr1=apt1.plot(x='sample_rate',y='time',kind='line',label='runtime(s)',c=col_time,marker=mymarker[1],lw=mymarkerlw, ax=ax1, legend=None)
    gt_time = apt1_gt.plot(x='sample_rate',y='time',kind='scatter',label='ref time',c=col_time,marker=mymarker[0], ax=ax1)

    lr2=apt1.plot(x='sample_rate',y='num_match',kind='line',label='#match',c=col_match,marker=mymarker[1],lw=mymarkerlw, ax=ax2, legend=None)
    gt_match = apt1_gt.plot(x='sample_rate',y='num_match',kind='scatter',label='ref match',c=col_match,marker=mymarker[0], ax=ax2)

    ax2.yaxis.set_major_locator(MaxNLocator(integer=True))

    # # legends

    ax1.xaxis.set_ticks(np.arange(0,0.1, 0.01))
    ax1.yaxis.set_ticks(np.arange(0,700, 50))
    ax2.yaxis.set_ticks(np.arange(0,12,1))

    ax1.set_xlabel('sample rate')
    ax1.set_ylabel('run time(s)')
    ax2.set_ylabel('# match with reference sample')

    handles,labels = [],[]
    for ax in fig.axes:
        for h,l in zip(*ax.get_legend_handles_labels()):
            print(h)
            print(l)
            handles.append(h)
            labels.append(l)

    ax1.get_legend().remove()
    ax2.get_legend().remove()

    plt.legend(handles, labels, fontsize='xx-small')

    plt.show()

    plt.savefig("lca_apt5.pdf", bbox_inches='tight')

   



if __name__=="__main__":
    main()
