from numpy import arange, power
import pylab as pl
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('PDF')


def main():
    sample_07 = pd.read_csv('patterns_07sample.csv')
    sample_05 = pd.read_csv('patterns_05sample.csv')
    sample_1 = pd.read_csv('patterns_nof1sample.csv')
    naive = pd.read_csv('patterns_naive.csv')

    ax = pl.subplot()

    sample_07['exp_desc'] = sample_07['exp_desc'].replace({'c921_debug_time__n1__nba__exist__005__2__2__05__varclus__t__s__07__2000': 2,
                                                           'c921_debug_time__n1__nba__exist__005__2__3__05__varclus__t__s__07__2000': 3,
                                                           'c921_debug_time__n1__nba__exist__005__2__1__05__varclus__t__s__07__2000': 1,
                                                           'c921_debug_time__n1__nba__exist__005__2__0__05__varclus__t__s__07__2000': 0
                                                           })

    sample_05['exp_desc'] = sample_05['exp_desc'].replace({'c921_05_f1sample__n1__nba__exist__005__2__0__05__varclus__t__s__05__2000': 0,
                                                           'c921_05_f1sample__n1__nba__exist__005__2__1__05__varclus__t__s__05__2000': 1,
                                                           'c921_05_f1sample__n1__nba__exist__005__2__2__05__varclus__t__s__05__2000': 2,
                                                           'c921_05_f1sample__n1__nba__exist__005__2__3__05__varclus__t__s__05__2000': 3
                                                           })

    sample_1['exp_desc'] = sample_1['exp_desc'].replace(
        {"c921_middle_opt_no_f1sample__n1__nba__exist__005__2__0__05__varclus__t__0__07__2000": 0,
         "c921_middle_opt_no_f1sample__n1__nba__exist__005__2__1__05__varclus__t__0__07__2000": 1,
         "c921_middle_opt_no_f1sample__n1__nba__exist__005__2__2__05__varclus__t__0__07__2000": 2,
         "c921_middle_opt_no_f1sample__n1__nba__exist__005__2__3__05__varclus__t__0__07__2000": 3
         }
    )

    naive['exp_desc'] = naive['exp_desc'].replace(
        {"c921_naive__n1__nba__exist__005__2__0__05__none__t__o__07__2000": 0,
         "c921_naive__n1__nba__exist__005__2__1__05__none__t__o__07__2000": 1,
         "c921_naive__n1__nba__exist__005__2__2__05__none__t__o__07__2000": 2,
         "c921_naive__n1__nba__exist__005__2__3__05__none__t__o__07__2000": 3
         }
    )

    sample_07['version'] = 'sample 0.7'
    sample_05['version'] = 'sample 0.5'
    sample_1['version'] = 'no sample'
    naive['version'] = 'naive'

    df_total = pd.concat([naive, sample_1, sample_07, sample_05])

    pl.ioff()

    ax = sns.boxplot(y='fscore', x='exp_desc',
                     data=df_total,
                     palette=('grey', 'red', 'green', 'blue'),
                     hue='version')

    ax.set_ylabel('fscore', fontsize=25)
    ax.set_xlabel('maximum_edges', fontsize=25)

    for tick in ax.xaxis.get_major_ticks():
        tick.label.set_fontsize(25)

    for tick in ax.yaxis.get_major_ticks():
        tick.label.set_fontsize(25)


    ax.yaxis.grid(which='major', linewidth=3.0, linestyle=':')
    ax.set_axisbelow(True)

    pl.show()
    pl.savefig("opt_naive_quality_comparison.pdf",bbox_inches='tight')


if __name__ == "__main__":
    main()
