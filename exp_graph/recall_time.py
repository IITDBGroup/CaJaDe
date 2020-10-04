from math import log10
from numpy import arange, power
import pylab as pl
import pandas as pd
import matplotlib
matplotlib.use('PDF')


def main():
    df = pd.read_csv('recall_effects_time.csv')
    df.sort_values(by='maximum_edges', inplace=True)

    recall_03 = [
        x for x in list(df.query('min_recall_threshold==\'0.3\'')['total'])]

    recall_05 = [
        x for x in list(df.query('min_recall_threshold==\'0.5\'')['total'])]

    recall_07 = [
        x for x in list(df.query('min_recall_threshold==\'0.7\'')['total'])]


    index = list(df['maximum_edges'].unique())

    # pl.ioff()

    compare = pd.DataFrame(
        {'recall:0.3': recall_03,
         'recall:0.5': recall_05,
         'recall:0.7': recall_07},
        index=index)

    ax = compare.plot.bar(color=('red', 'green', 'blue'))

    legend = ax.legend(bbox_to_anchor=(0.005, 1.04), prop={'size': 20},
                       labels=['recall thresh:0.3',
                               'recall thresh:0.5', 'recall thresh:0.7'],
                       loc=2, borderpad=0.1, labelspacing=0, handlelength=1, handletextpad=0.2,
                       columnspacing=0.5, framealpha=1)
    legend.get_frame().set_edgecolor('black')

    # axis labels and tics
    ax.set_ylabel('time seconds', fontsize=25)
    ax.set_xlabel('maximum_edges', fontsize=25)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(25)

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(25)

    pl.xticks(rotation=0)

    # grid
    ax.yaxis.grid(which='major', linewidth=3.0, linestyle=':')
    ax.set_axisbelow(True)
    ax.set_yscale("log", nonposy='clip', basey=10)
    pl.show()
    pl.savefig("recall_time.pdf", bbox_inches='tight')


if __name__ == "__main__":
    main()
