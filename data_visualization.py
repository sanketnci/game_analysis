from bokeh.plotting import figure, output_file, show
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral6
from bokeh.palettes import Category10
from bokeh.plotting import figure
import pandas as pd
import mysql.connector
import re
from db_connect import DBConnect


def convert_to_hours(time_string):
    hours_match = re.findall('(\d+)h', time_string)
    minutes_match = re.findall('(\d+)m', time_string)
    seconds_match = re.findall('(\d+)s', time_string)

    hours = int(hours_match[0]) if hours_match else 0
    minutes = int(minutes_match[0]) if minutes_match else 0
    seconds = int(seconds_match[0]) if seconds_match else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    total_hours = total_seconds / 3600

    return total_hours


class Visuals:

    def __init__(self, cr):
        self.cursor = cr

    def average_time_take_platforms(self):
        """
         Average time taken on platforms

        """
        self.cursor.execute("SELECT * FROM platforms")
        rows = self.cursor.fetchall()
        self.cursor.execute("SHOW COLUMNS FROM platforms")
        column_names = [col[0] for col in self.cursor.fetchall()]
        platforms_df = pd.DataFrame(rows, columns=column_names)

        platforms_df['fastest'] = platforms_df['fastest'].apply(convert_to_hours)

        platform_data = platforms_df.groupby('platform')['fastest'].mean().reset_index()

        platform_data = platform_data.sort_values(by='fastest', ascending=False)

        # create a ColumnDataSource object
        source = ColumnDataSource(platform_data)

        # create a figure
        p = figure(title='Average Time Taken to Complete Game by Platform', x_range=platform_data['platform'],
                   plot_height=400, plot_width=800)

        # add a vertical bar chart to the figure
        p.vbar(x='platform', top='fastest', width=0.9, source=source, line_color='white',
               fill_color=factor_cmap('platform', palette=Category10[10], factors=platform_data['platform']))

        # customize the plot
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.2
        p.title.text_font_size = '18pt'
        p.xaxis.axis_label = 'Platform'
        p.yaxis.axis_label = 'Average Time Taken (hours)'

        # show the plot
        show(p)

    def genres_count(self):
        """
        top10 genres with most games plot
        """

        self.cursor.execute("SELECT * FROM games_data_hltb")
        rows = self.cursor.fetchall()

        self.cursor.execute("SHOW COLUMNS FROM games_data_hltb")
        column_names = [col[0] for col in self.cursor.fetchall()]

        games_df = pd.DataFrame(rows, columns=column_names)
        games_df.head()

        games_df['genres'] = games_df['genres'].apply(lambda x: x.split(','))
        games_df = games_df.explode('genres')
        # .split(',') [v for v in x] .apply(pd.Series.explode)

        # games_df.head()

        # grouped_df = games_df.groupby('genres').count().reset_index()
        # grouped_df.sort_values(by=['review_score'],ascending=False)

        """ --------------- top10 genres with most games plot-----------------"""
        grouped_df = games_df.value_counts(subset=['genres'])
        grouped_df = grouped_df.to_frame('count')
        grouped_df = grouped_df.reset_index()
        grouped_df = grouped_df[0:10]
        grouped_df = grouped_df.replace('', 'Miscellaneous')
        print(grouped_df)

        # create a ColumnDataSource from the data
        source = ColumnDataSource(data=grouped_df)

        # create a figure object
        p = figure(x_range=grouped_df['genres'], plot_height=400, plot_width=800, title='Top 10 Genres with most games')

        # add a vertical bar glyph
        p.vbar(x='genres', top='count', width=0.8, source=source, color='#99c2ff')

        # add a hover tool to display count value on mouse over
        p.add_tools(HoverTool(tooltips=[('Count', '@count')]))

        # customize the plot
        p.title.text_font_size = '20pt'
        p.xaxis.major_label_text_font_size = '12pt'
        p.yaxis.major_label_text_font_size = '12pt'
        p.xaxis.major_label_orientation = 'vertical'
        p.xaxis.axis_label_text_font_size = '14pt'
        p.yaxis.axis_label_text_font_size = '14pt'
        p.yaxis.axis_label = 'Count'

        # output to static HTML file
        # output_file('genres_count.html')

        # show the plot
        show(p)

    def max_time_platform(self):
        self.cursor.execute("SELECT * FROM platforms")
        rows = self.cursor.fetchall()
        self.cursor.execute("SHOW COLUMNS FROM platforms")
        column_names = [col[0] for col in self.cursor.fetchall()]
        platforms_df = pd.DataFrame(rows, columns=column_names)
        # Assume df is your pandas dataframe
        platforms_df['fastest'] = platforms_df['fastest'].apply(convert_to_hours)

        platform_data = platforms_df.groupby('platform')['fastest'].max().reset_index()

        platform_data = platform_data.sort_values(by='fastest', ascending=False)

        # create a ColumnDataSource object
        source = ColumnDataSource(platform_data)

        # create a figure
        p = figure(title='Maximum Time Taken to Complete Game by Platform', x_range=platform_data['platform'],
                   plot_height=400, plot_width=800)

        # add a vertical bar chart to the figure
        p.vbar(x='platform', top='fastest', width=0.9, source=source, line_color='white',
               fill_color=factor_cmap('platform', palette=Category10[10], factors=platform_data['platform']))

        # customize the plot
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.2
        p.title.text_font_size = '18pt'
        p.xaxis.axis_label = 'Platform'
        p.yaxis.axis_label = 'Average Time Taken (hours)'
        show(p)


if __name__ == '__main__':
    db = DBConnect()
    cursors = db.cursor
    vs = Visuals(cursors)
    vs.average_time_take_platforms()
    vs.genres_count()
    vs.max_time_platform()