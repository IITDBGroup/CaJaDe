from math import log10
from numpy import arange, power
import pylab as pl
import pandas as pd
import matplotlib
matplotlib.use('PDF')


def main():
    df = pd.read_csv('naive_vs_optimized_time.csv')
    df.sort_values(by='maximum_edges', inplace=True)

    naive = [
        x for x in list(df.query('type==\'naive\'')['total'])]
    optimized_no_f1sample = [
        x for x in list(df.query('type==\'nosample\'')['total'])]
    optimized_07_sample = [
        x for x in list(df.query('type==\'Sample_0.7\'')['total'])]
    optimized_05_sample = [
        x for x in list(df.query('type==\'Sample_0.5\'')['total'])]

    index = list(df['maximum_edges'].unique())

    # pl.ioff()

    compare = pd.DataFrame(
        {'naive': naive,
         'opt:no sample': optimized_no_f1sample,
         'opt:0.7 f1 sample rate': optimized_07_sample,
         'opt:0.5 f1 sample rate': optimized_05_sample},
        index=index)

    ax = compare.plot.bar(color=('grey', 'red', 'green', 'blue'))

    legend = ax.legend(bbox_to_anchor=(0.005, 1.04), prop={'size': 20},
                       labels=['naive', 'opt w/o sample',
                               'opt:0.7 f1 sample', 'opt:0.5 f1 sample'],
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
    ax.set_yscale("log", nonposy='clip', basey=10)
    pl.show()
    pl.savefig("opt_vs_naive.pdf", bbox_inches='tight')


if __name__ == "__main__":
    main()
