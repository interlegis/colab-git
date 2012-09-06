#!/usr/bin/env python
import subprocess
import os
import shlex
import solr
import iso8601
import sys

def exec_cmd(command, cwd='/tmp'):
    return ''.join(subprocess.Popen(shlex.split(command),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    cwd=cwd).communicate())

def get_msg(lines):
    msg_lines = []
    line = lines.pop()
    while line != '==@@@==@@@==':
        msg_lines += [line]
        line = lines.pop()
    return '\n'.join(msg_lines)

author_dict = {'Marcio Mazza': 'mazza',
               }

def get_author(line):
    # TODO: ajeitar isso !!!!
    return author_dict.get(line, line)

def to_docs(repo, lines):
    while lines:
        abbrev_commmit_hash, commmit_hash = lines.pop().split()
        author = get_author(lines.pop())
        date = iso8601.parse_date(lines.pop())
        msg = get_msg(lines)
        yield {'Creator': author,
               'Title': msg,
               'Type': 'changeset',
               'UID': 'CHANGESET_' + commmit_hash,
               'getId': abbrev_commmit_hash,
               'path_string': u'/changeset/%s/%s' % (commmit_hash, repo),
               'created': date,
               'modified': date,
               }

def atualizar_solr(solr_url, repo, repo_dir):
    lines = exec_cmd("git log --format='%h %H%n%an%n%ai%n%B%n==@@@==@@@=='",
                     repo_dir).splitlines()
    lines.reverse()

    solr_conn = solr.SolrConnection(solr_url)
    for doc in to_docs(repo, lines):
        print 'Adicionando [%s]' % doc['path_string']
        print doc
        solr_conn.add(**doc)
    print 'Enviando...'
    solr_conn.commit()

def reindex(repos_base_dir, solr_url):
    print 'Sincronizando...'
    print ' * Diretorio de repos: ', repos_base_dir
    print ' * URL Solr: ', solr_url
    print
    subdirs = os.listdir(repos_base_dir)
    repos = [d[:-len('.git')] for d in subdirs if d.endswith('.git')]
    for repo in repos:
        repo_dir = '%s/%s.git' % (repos_base_dir, repo)

        print '## repo [%s]: sincronizando com a origem...' % repo
        saida_update = exec_cmd('git remote update', repo_dir)
        print saida_update

        if saida_update == 'Fetching origin\n':
            print '## repo [%s]: Nao houve mudanca' % repo
        else:
            print '## repo [%s]: atualizando solr...' % repo
            atualizar_solr(solr_url, repo, repo_dir)
        print

if __name__ == '__main__':
    # Chamar este script passando: repos_base_dir, solr_url
    reindex(sys.argv[1], sys.argv[2])
