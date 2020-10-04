from numpy import arange, power
import pylab as pl
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('PDF')


def main():

    recall_df = pd.read_csv('recall_effects_patterns.csv')

    ax = pl.subplot()

    recall_03 = recall_df[(recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__0__03__varclus__t__o__07__2000')|
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__1__03__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__2__03__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__3__03__varclus__t__o__07__2000')]

    recall_03['exp_desc'] = recall_03['exp_desc'].replace({'c921_recall_threshold__n1__nba__exist__005__2__0__03__varclus__t__o__07__2000': 0,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__1__03__varclus__t__o__07__2000': 1,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__2__03__varclus__t__o__07__2000': 2,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__3__03__varclus__t__o__07__2000': 3
                                                           })

    recall_05 = recall_df[(recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__0__05__varclus__t__o__07__2000')|
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__1__05__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__2__05__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__3__05__varclus__t__o__07__2000')]


    recall_05['exp_desc'] = recall_05['exp_desc'].replace({'c921_recall_threshold__n1__nba__exist__005__2__0__05__varclus__t__o__07__2000': 0,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__1__05__varclus__t__o__07__2000': 1,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__2__05__varclus__t__o__07__2000': 2,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__3__05__varclus__t__o__07__2000': 3
                                                           })


    recall_07 = recall_df[(recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__0__07__varclus__t__o__07__2000')|
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__1__07__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__2__07__varclus__t__o__07__2000') |
                            (recall_df['exp_desc']=='c921_recall_threshold__n1__nba__exist__005__2__3__07__varclus__t__o__07__2000')]

    recall_07['exp_desc'] = recall_07['exp_desc'].replace({'c921_recall_threshold__n1__nba__exist__005__2__0__07__varclus__t__o__07__2000': 0,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__1__07__varclus__t__o__07__2000': 1,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__2__07__varclus__t__o__07__2000': 2,
                                                           'c921_recall_threshold__n1__nba__exist__005__2__3__07__varclus__t__o__07__2000': 3
                                                           })


    recall_07['version'] = 'recall thresh: 0.7'
    recall_05['version'] = 'recall thresh: 0.5'
    recall_03['version'] = 'recall thresh: 0.3'

    df_total = pd.concat([recall_07, recall_05, recall_03])

    pl.ioff()

    ax = sns.boxplot(y='fscore', x='exp_desc',
                     data=df_total,
                     palette=('green', 'red', 'blue'),
                     hue='version')

    ax.set_ylabel('fscore', fontsize=25)
    ax.set_xlabel('maximum_edges', fontsize=25)
    ax.yaxis.grid(which='major', linewidth=3.0, linestyle=':')
    ax.set_axisbelow(True)


    pl.show()
    pl.savefig("recall_thresh_box_plot.pdf",bbox_inches='tight')


if __name__ == "__main__":
    main()
