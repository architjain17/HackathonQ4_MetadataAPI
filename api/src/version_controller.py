import os
import json
import csv
from git import Git
from git import GitCommandError
from datetime import datetime
from Crypto.Cipher import AES

# local imports
from db_connector import engine


def _read_config():
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../config/encryption_config.json')
    with open(config_file) as cf:
        config = json.load(cf)

    return config


def _write_csv(csv_filename):
    conn = engine.connect()
    query = """SELECT * FROM Util_raw.dwdatapoint"""
    results = conn.execute(query)

    with open(csv_filename, 'wb') as csv_file:
        # creating csv object
        csv_obj = csv.writer(csv_file, delimiter=',')
        # dump column titles
        csv_obj.writerow([header.name for header in results._cursor_description()])
        # dumping to csv
        for row in results:
            csv_obj.writerow([col.encode('utf8') for col in row])

    results.close()
    conn.close()


def _encrypt_csv(in_file, out_file):
    # reading config for encryption
    config = _read_config()
    encryption_obj = AES.new(config['key'], AES.MODE_CBC, config['iv'])

    key_length = 32
    bs = AES.block_size
    cipher = AES.new(config['key'], AES.MODE_CBC, config['iv'])

    with open(in_file, 'rb') as in_file, open(out_file, 'wb') as out_file:
        finished = False
        while not finished:
            chunk = in_file.read(1024 * bs)
            if len(chunk) == 0 or len(chunk) % bs != 0:
                padding_length = (bs - len(chunk) % bs) or bs
                chunk += padding_length * chr(padding_length)
                finished = True
            out_file.write(cipher.encrypt(chunk))


def save_version(username, comments):
    github_repo = Git(os.path.dirname(os.path.realpath(__file__)))
    github_active_branch = github_repo.execute(['git', 'rev-parse', '--abbrev-ref', 'HEAD'])

    # pulling repo to latest commit
    #_git_pull(github_repo, github_active_branch)

    temp_csv_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data/DWDataPoint_temp.csv')
    csv_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../data/DWDataPoint.csv')

    # extracting csv
    _write_csv(temp_csv_filename)

    # encrypting csv
    _encrypt_csv(temp_csv_filename, csv_filename)

    # pushing csv to github
    _git_push(github_repo, github_active_branch, csv_filename, username, comments)


def _git_pull(github_repo, github_active_branch):

    print 'Currently working on branch {0}'.format(github_active_branch)
    print 'Pulling branch {0} from remote'.format(github_active_branch)

    try:
        github_repo.execute(['git', 'pull', 'origin', github_active_branch])
    except GitCommandError as e:
        print 'Error while pulling from git'
        print e
        print 'Please resolve above error in order to continue'
        exit(1)


def _git_push(github_repo, github_active_branch, csv_filename, username, comments):

    # again pulling to get to the latest commit
    #_git_pull(github_repo, github_active_branch)

    print 'Adding csv to git staging area on branch {0}'.format(github_active_branch)
    try:
        github_repo.execute(['git', 'add', csv_filename])
    except GitCommandError as e:
        print 'Error while adding files in git'
        print e
        print 'Please resolve above error in order to continue'
        exit(1)

    print 'Committing csv to git on branch {0}'.format(github_active_branch)
    try:
        commit_msg = '{0} by {1} at {2}'.format(comments, username, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        github_repo.execute(['git', 'commit', '-m', commit_msg])
    except GitCommandError as e:
        print 'Error while adding files in git'
        print e
        print 'Please resolve above error in order to continue'
        exit(1)

    print 'Pushing branch {0} to remote'.format(github_active_branch)

    try:
        print 'git push'
        #github_repo.execute(['git', 'push', 'origin', github_active_branch])
    except GitCommandError as e:
        print 'Error while pushing to git'
        print e
        print 'Please resolve above error in order to continue'
        exit(1)
