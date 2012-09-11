#!/usr/bin/env python
#
# Dependencias: solrpy iso8601
#
import datetime
import os
import shlex
import subprocess
import sys

import iso8601
import solr


def exec_cmd(command, cwd='/tmp'):
    return ''.join(subprocess.Popen(shlex.split(command),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    cwd=cwd).communicate())

# TODO: consultar colab !!!!
author_dict = {'davilima6@gmail.com': 'davilima6',
               'marcospaulosp10@gmail.com' : 'marcospaulosp10',
               'mazza@interlegis.leg.br' : 'mazza',
               'thiagobernadino@interlegis.leg.br': 'thiagocesar',
               }

def get_author(name, email):
    return author_dict.get(email, name)

def to_docs(repo, lines):
    while lines:
        abbrev_commmit_hash, commmit_hash = lines.pop().split()
        author = get_author(lines.pop(), lines.pop())
        date = iso8601.parse_date(lines.pop())
        subject = lines.pop()
        yield {
            'Creator': author,
            'collaborator': author,
            'UID': 'CHANGESET_' + commmit_hash,
            'getId': abbrev_commmit_hash,
            'Type': 'changeset',
            'path_string': u'/changeset/%s/%s' % (commmit_hash, repo),
            'Title': '[%s] - %s' % (abbrev_commmit_hash, subject),
            'created': date,
            'modified': date,
            }

def atualizar_solr(solr_url, repo, repo_dir):
    lines = exec_cmd("git log --format='%h %H%n%an%ae%n%ai%n%s'",
                     repo_dir).splitlines()
    lines.reverse()

    solr_conn = solr.SolrConnection(solr_url)
    for doc in to_docs(repo, lines):
        print 'Adicionando [%s]' % doc['path_string']
        print doc
        solr_conn.add(**doc)
    print 'Enviando ao Solr...'
    solr_conn.commit()

def reindex(repos_base_dir, solr_url):
    print '################################################################'
    print 'Sincronizando (%s)' % datetime.datetime.now()
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
        print 'Finalizado (%s)' % datetime.datetime.now()
        print

if __name__ == '__main__':
    # Chamar este script passando: repos_base_dir, solr_url
    reindex(sys.argv[1], sys.argv[2])

