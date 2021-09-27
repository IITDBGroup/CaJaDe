from math import log10
from numpy import arange, power
import pylab as pl
import pandas as pd
import matplotlib
matplotlib.use('PDF')


def main():
    df = pd.read_csv('april_nba_sample_time.csv')
    df.sort_values(by='maximum_edges', inplace=True)

    optimized_no_f1sample = [
        x for x in list(df.query('f1_sample_rate==1')['total'])]
    print(optimized_no_f1sample)

    optimized_05_sample= [
        x for x in list(df.query('f1_sample_rate==0.5')['total'])]
    print(optimized_05_sample)

    optimized_03_sample = [
        x for x in list(df.query('f1_sample_rate==0.3')['total'])]
    print(optimized_03_sample)

    optimized_01_sample = [
        x for x in list(df.query('f1_sample_rate==0.1')['total'])]
    print(optimized_01_sample)

    index = list(df['maximum_edges'].unique())

    # pl.ioff()

    compare = pd.DataFrame({'opt:no sample': optimized_no_f1sample,
         'opt:0.5 f1 sample rate': optimized_05_sample,
         'opt:0.3 f1 sample rate': optimized_03_sample,
         'opt:0.1 f1 sample rate': optimized_01_sample},
        index=index)

    ax = compare.plot.bar(color=('grey', 'red', 'green', 'blue'))

    legend = ax.legend(bbox_to_anchor=(0.005, 1.04), prop={'size': 20},
                       labels=['opt w/o sample', 'opt:0.5 f1 sample',
                               'opt:0.3 f1 sample', 'opt:0.1 f1 sample'],
                       loc=2, borderpad=0.1, labelspacing=0, handlelength=1, handletextpad=0.2,
                       columnspacing=0.5, framealpha=1)
    legend.get_frame().set_edgecolor('black')

    # axis labels and tics
    ax.set_ylabel('time seconds', fontsize=30)
    ax.set_xlabel('maximum_edges', fontsize=30)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(30)

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(30)

    pl.xticks(rotation=0)

    # grid
    ax.yaxis.grid(which='major', linewidth=3.0, linestyle=':')
    ax.set_axisbelow(True)
    # ax.set_yscale("log", nonposy='clip', basey=10)
    pl.show()
    pl.savefig("opt_vs_naive.pdf", bbox_inches='tight')


if __name__ == "__main__":
    main()
