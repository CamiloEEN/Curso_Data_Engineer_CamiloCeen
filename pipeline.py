import logging
logging.basicConfig(level=logging.INFO)
import subprocess
import datetime

logger = logging.getLogger(__name__)
news_sites_uids=['elespectador','elpais']

def main():
    _extract()
    _transform()
    _load()


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        subprocess.run(['python', 'main.py',news_site_uid], cwd='./web_scrapper_curso_data_eng')
        subprocess.run(['Powershell','Get-ChildItem','-Path','.', '-Recurse',
                        '-Include', f'{news_site_uid}*', r'|', 'Move-Item', '-Destination',
                        '../Transform_data'],
                       cwd='./web_scrapper_curso_data_eng')

def _transform():
    logger.info('Starting transform process')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = '{}_{}_articles.csv'.format(news_site_uid,now)
        clean_data_filename = 'clean_{}'.format(dirty_data_filename)
        subprocess.run(['python','newspaper_recipe.py', dirty_data_filename], cwd='./Transform_data')
        subprocess.run(['Powershell','Get-ChildItem','-Path','.', '-Recurse',
                        '-Include', f'{dirty_data_filename}*', r'|', 'Remove-Item'],
                       cwd='./Transform_data')
        #subprocess.run(['rm', dirty_data_filename], cwd='./Transform_data')
        #subprocess.run(['mv', clean_data_filename, '../Newspaper_data_to_SQLite/{}.csv'.format(news_site_uid)],
        #               cwd='./Transform_data')
        subprocess.run(['Powershell','Get-ChildItem','-Path','.', '-Recurse',
                        '-Include', f'{clean_data_filename}*', r'|', 'Move-Item', '-Destination',
                        '../Newspaper_data_to_SQLite'],
                       cwd='./Transform_data')
    

def _load():
    logger.info('Starting load process')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        clean_data_filename = 'clean_{}_{}_articles.csv'.format(news_site_uid,now)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./Newspaper_data_to_SQLite' )
        #subprocess.run(['rm',clean_data_filename], cwd='./Newspaper_data_to_SQLite')
        subprocess.run(['Powershell','Get-ChildItem','-Path','.', '-Recurse',
                        '-Include', f'{clean_data_filename}*', r'|', 'Remove-Item'],
                       cwd='./Newspaper_data_to_SQLite')

if __name__=='__main__':
    main()