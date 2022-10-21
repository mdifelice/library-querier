import ast
import csv
import datetime
import hashlib
import json
import math
import os.path as path
import re
import tempfile
import time
import urllib.parse as parse
import urllib.request as request

def query( search_terms, output, start_year = 1900, end_year = datetime.date.today().year, max_attempts = 3, ignore_failed_calls = False, use_cache = False, selected_apis = [], debug = False, api_keys = {}  ):
	articles = {}

	if ( path.exists( output ) ):
		with open( output ) as f:
			reader = csv.reader( f )

			for row in reader:
				try:
					article = {
						'title'        : row[0],
						'source'       : ast.literal_eval( row[1] ) if ( row[1] ) else [],
						'authors'      : ast.literal_eval( row[2] ) if ( row[2] ) else [],
						'year'         : int( row[3] ),
						'doi'          : row[4],
						'search_terms' : ast.literal_eval( row[5] ) if ( row[5] ) else [],
						'rank'         : ast.literal_eval( row[6] ) if ( row[6] ) else [],
						'date'         : ast.literal_eval( row[7] ) if ( row[7] ) else []
					}

					article_id = __get_article_index( article )

					articles[ article_id ] = article
				except Exception as e:
					__message( 'Invalid data on input file: ' + str( e ), debug )

	old_articles         = len( articles )
	added_articles       = 0
	updated_articles_map = {}

	def ieeexplore_parse_articles( response, use_cache, ignore_failed_calls, max_attempts, debug ):
		articles = []

		raw_articles = response.get( 'articles' )

		if raw_articles:
			for raw_article in raw_articles:
				authors_container = raw_article.get( 'authors' )

				if authors_container:
					authors = authors_container.get( 'authors' )

				if not authors:
					authors = []

				articles.append( {
					'title'   : raw_article.get( 'title' ),
					'authors' : list( map( lambda author : author.get( 'full_name' ), authors ) ),
					'year'    : int( raw_article.get( 'publication_year' ) ),
					'doi'     : __parse_doi( raw_article.get( 'doi' ) )
				} )

		return articles

	def pubmed_parse_articles( response, use_cache, ignore_failed_calls, max_attempts, debug ):
		articles = []

		searchresult = response.get( 'esearchresult' )

		if searchresult:
			idlist       = searchresult.get( 'idlist' ) if 'idlist' in searchresult else ''

			summary_response = __request_url( 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=' + parse.quote( ','.join( idlist ) ) + '&retmode=json', use_cache, ignore_failed_calls, max_attempts, debug )

			if summary_response:
				decoded_summary_response = json.loads( summary_response )

				if (
					decoded_summary_response
					and 'result' in decoded_summary_response
				):
					result = decoded_summary_response.get( 'result' )

					for id in result.get( 'uids' ):
						raw_article = result.get( id )
						doi         = None
						article_ids = raw_article.get( 'articleids' )

						for article_id in article_ids:
							if article_id.get( 'idtype' ) == 'doi':
								doi = article_id.get( 'value' )

								break
 
						article = {
							'title'   : raw_article.get( 'title' ),
							'authors' : list( map( lambda author : author.get( 'name' ), raw_article.get( 'authors' ) ) ),
							'year'    : datetime.datetime.strptime( raw_article.get( 'sortpubdate' ), '%Y/%m/%d %H:%M' ).year,
							'doi'     : __parse_doi( doi )
						}

						articles.append( article )

		return articles

	def scopus_parse_articles( response, use_cache, ignore_failed_calls, max_attempts, debug ):
		articles = []

		search_results = response.get( 'search-results' )

		if search_results:
			for entry in search_results.get( 'entry' ):
				if 'error' not in entry:
					article = {
						'title'   : entry.get( 'dc:title' ),
						'authors' : entry.get( 'dc:creators' ),
						'year'    : datetime.datetime.strptime( entry.get( 'prism:coverDate' ), '%Y-%m-%d' ).year,
						'doi'     : __parse_doi( entry.get( 'prism:doi' ) )
					}

					articles.append( article )

		return articles

	def eric_parse_articles( response, use_cache, ignore_failed_calls, max_attempts, debug ):
		articles = []

		response = response.get( 'response' )

		if response:
			for doc in response.get( 'docs' ):
				article = {
					'title'   : doc.get( 'title' ),
					'authors' : doc.get( 'authors' ),
					'year'    : int( doc.get( 'publicationyear' ) ),
					'doi'     : doc.get( 'url' )
				}

				articles.append( article )

		return articles

	def doaj_parse_articles( response, use_cache, ignore_failed_calls, max_attempts, debug ):
		articles = []

		results = response.get( 'results' )

		if results:
			for result in results:
				data = result.get( 'bibjson' )

				if data:
					doi         = None
					authors     = []
					identifiers = data.get( 'identifier' )
					raw_authors = data.get( 'author' )

					if identifiers:
						for identifier in identifiers:
							if 'DOI' == identifier.get( 'type' ):
								doi = identifier.get( 'id' )

								break

					if raw_authors:
						for raw_author in raw_authors:
							name = raw_author.get( 'name' )

							if name:
								authors.append( name )

					article = {
						'title'   : data.get( 'title' ),
						'authors' : authors,
						'year'    : int( data.get( 'year' ) ),
						'doi'     : __parse_doi( doi )
					}

				articles.append( article )

		return articles

	apis = {
# @link https://developer.ieee.org/docs/read/Searching_the_IEEE_Xplore_Metadata_API
		'ieeexplore' : {
			'parse_articles' : ieeexplore_parse_articles,
			'parse_total'    : lambda response : response.get( 'total_records' ) if 'total_records' in response else 0,
			'request_mask'   : 'http://ieeexploreapi.ieee.org/api/v1/search/articles?apikey={api_key}&format=json&max_records={count}&start_record={start}&index_terms={search_terms}&start_year={start_year}&end_year={end_year}'
		},
# @link https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch
		'pubmed' : {
			'parse_arguments' : {
				'search_terms' : lambda value, arguments : re.sub( '"[^"]+"', '$0[All Fields]', value )
			},
			'parse_articles' : pubmed_parse_articles,
			'parse_total'    : lambda response : response.get( 'esearchresult' ).get( 'count' ) if 'esearchresult' in response and 'count' in response.get( 'esearchresult' ) else 0,
			'request_mask'   : 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retstart={start}&retmax={count}&retmode=json&term={search_terms}&mindate={start_year}&maxdate={end_year}'
		},
# @link https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl
		'scopus' : {
			'parse_arguments' : {
				'end_year' : lambda value, arguments : '-' + str( value ) if 'start_year' in arguments else str( value )
			},
			'parse_articles' : scopus_parse_articles,
			'parse_total'    : lambda response : response.get( 'search-results' ).get( 'opensearch:totalResults' ) if 'search-results' in response and 'opensearch:totalResults' in response.get( 'search-results' ) else 0,
			'request_mask'   : 'https://api.elsevier.com/content/search/scopus?apiKey={api_key}&httpAccept=application/json&count={count}&start={start}&query=KEY%28{search_terms}%29&date={start_year}{end_year}'
		},
# @link https://eric.ed.gov/?api#/default/get_eric_
# No date filters
		'eric' : {
			'parse_arguments' : {
				'search_terms' : lambda value, arguments : ' OR '.join( map( lambda field : '(' + ' AND '.join( map( lambda value : field + ':' + value, value.split( '" "' ) ) ) + ')', [ 'title', 'subject', 'description' ] ) ),
			},
			'parse_articles' : eric_parse_articles,
			'parse_total'    : lambda response : response.get( 'response' ).get( 'numFound' ) if 'response' in response and 'numFound' in response.get( 'response' ) else 0,
			'request_mask'   : 'https://api.ies.ed.gov/eric/?search={search_terms}%20&format=json&rows={count}&start={start}&fields=title,author,publicationdateyear,url'
		},
# @todo @link https://doaj.org/api/docs
		'doaj' : {
			'parse_articles' : doaj_parse_articles,
			'parse_total'    : lambda response : response.get( 'total' ) if 'total' in response else 0,
			'request_mask'   : 'https://doaj.org/api/search/articles/{search_terms}?pageSize={count}&page={page}',
		},
	}

	for api in apis:
		settings = apis[ api ]

		if (
			not selected_apis
			or api in selected_apis
		):
			__message( 'Calling API ' + api + ' for search terms: "' + search_terms + '"...', True )

			processed_articles = 0
			queried_articles   = 0
			articles_per_page  = 25
			total_articles     = None

			while (
				total_articles is None
				or queried_articles < total_articles
			):
				placeholders = {
					'api_key'      : api_keys[ api ] if api in api_keys else '',
					'count'        : articles_per_page,
					'end_year'     : end_year,
					'page'         : math.floor( queried_articles / articles_per_page ) + 1,
					'search_terms' : search_terms,
					'start'        : queried_articles,
					'start_year'   : start_year
				}

				queried_articles += articles_per_page

				def request_parser( matches ):
					argument = matches[1]
					value    = ''

					if argument in placeholders:
						value = placeholders.get( argument )

						arguments_parser = settings.get( 'parse_arguments' )

						if (
							arguments_parser is not None
							and argument in arguments_parser
					    ):
							value = arguments_parser.get( argument )( value, placeholders )

					return parse.quote( str( value ) )

				request = re.sub(
					'{([^}]+)}',
					request_parser,
					settings.get( 'request_mask' )
				)

				response = __request_url( request, use_cache, ignore_failed_calls, max_attempts, debug )

				if response:
					decoded_response = json.loads( response )

					if decoded_response:
						if total_articles is None:
							total_articles = int( settings.get( 'parse_total' )( decoded_response ) )

							__message( 'Total articles: ' + str( total_articles ), debug )

							__start_progress( 'Receiving articles...', total_articles )
						
						page_articles = settings.get( 'parse_articles' )( decoded_response, use_cache, ignore_failed_calls, max_attempts, debug  )

						for page_article in page_articles:
							__update_progress()

							processed_articles += 1
							updated_article    = None

							article_id = __get_article_index( page_article )

							if article_id not in articles:
								year = page_article.get( 'year' )

								if (
									year >= start_year
									and year <= end_year
								):
									article = {
										'title'        : page_article.get( 'title' ),
										'source'       : [],
										'authors'      : page_article.get( 'authors' ),
										'year'         : year,
										'doi'          : page_article.get( 'doi' ),
										'search_terms' : [],
										'rank'         : [],
										'date'         : [],
									}

									added_articles += 1
								else:
									article = None
							else:
								article = articles.get( article_id )

								updated_article = article_id

							if article:
								source_index = -1

								source = article.get( 'source' )

								if source:
									maybe_source_index = 0

									for article_source in source:
										if (
											article_source == api
											and article.get( 'search_terms' )[ maybe_source_index ] == search_terms
										):
											source_index = maybe_source_index

										maybe_source_index += 1
									

								rank             = processed_articles
								date             = datetime.datetime.now().strftime( '%Y-%m-%d' )
								article_modified = False

								if source_index == -1:
									article_source       = article.get( 'source' )
									article_search_terms = article.get( 'search_terms' )
									article_rank         = article.get( 'rank' )
									article_date         = article.get( 'date' )

									article_source.append( api )
									article_search_terms.append( search_terms )
									article_rank.append( rank )
									article_date.append( date )

									article.update( {
										'source' 	   : article_source,
										'search_terms' : article_search_terms,
										'rank' 		   : article_rank,
										'date' 		   : article_date
									} )

									article_modified = True
								else:
									article_rank = article.get( 'rank' )
									article_date = article.get( 'date' )

									if (
										article_rank[ source_index ] != rank
										or article_date[ source_index ] != date
									):
										article_rank[ source_index ] = rank
										article_date[ source_index ] = date

										article.update( {
											'rank' : article_rank,
											'date' : article_date
										} )

										article_modified = True

								if article_modified:
									articles[ article_id ] = article

									if updated_article:
										updated_articles_map[ article_id ] = True

			if total_articles is not None:
				__finish_progress()

	total_articles_by_provider = {}

	with open( output, 'w' ) as f:
		if ( len( articles ) ):
			writer = csv.DictWriter( f, fieldnames = list( next( iter( articles.values() ) ).keys() ) )

			print(list( next( iter( articles.values() ) ).keys() ) )
			writer.writerows( articles.values() )

			for id in articles:
				article = articles.get( id )

				sources = set( article.get( 'source' ) )

				for source in sources:
					if source not in total_articles_by_provider:
						total_articles_by_provider[ source ] = 0

					total_articles_by_provider[ source ] += 1
	
		__message( 'Added articles: ' + str( added_articles ) + '\nUpdated articles: ' + str( len( updated_articles_map ) ) + '\nTotal articles: ' + str( old_articles + added_articles ) + ( ' (' + ', '.join( map( lambda key : key + ': ' + str( total_articles_by_provider.get( key ) ), total_articles_by_provider.keys() ) ) + ')' if total_articles_by_provider else '' ), True )

def __parse_doi( doi ):
	return 'https://doi.org/' + doi if doi else ''

def __md5( string ):
	return hashlib.md5( string.encode( 'utf-8' ) ).hexdigest()

def __message( message, debug ):
	if ( debug ):
		print( message )

def __get_article_index( article ):
	doi = article.get( 'doi' )

	if doi:
		hash_seed = doi
	else:
		authors = article.get( 'authors' )

		if authors:
			author = authors[0]
		else:
			author = ''

		hash_seed = article.get( 'title' ) + author + str( article.get( 'year' ) )

	return __md5( hash_seed )

def __start_progress( title, total ):
	global __progress_total, __progress_title, __progress

	__progress_total = int( total )
	__progress_title = title
	__progress       = 0

	__print_progress()

def __print_progress( newline = False ):
	global __progress_total, __progress_title, __progress

	print( '%s [%.2f%%]' % ( __progress_title, __progress * 100 / __progress_total if __progress_total else 0 ), end = '\r' if not newline else '\n' )

def __update_progress():
	global __progress

	__progress += 1

	__print_progress()

def __finish_progress():
	global __progress_total, __progress

	__progress = __progress_total

	__print_progress( newline = True )

def __file_get_contents( filename ):
	contents = None

	with open( filename ) as f:
		contents = f.read()
	
	return contents

def __request_url( url, use_cache, ignore_failed_calls, max_attempts, debug ):
	cache_file = tempfile.gettempdir() + '/library-querier-' + __md5( url ) + '.tmp'
	response = None

	if (
		use_cache
		and path.exists( cache_file )
		and path.getmtime( cache_file ) > ( time.time() - ( 24 * 60 * 60 ) )
	):
		response = __file_get_contents( cache_file )

	if response is None:
		attempts = 0

		while attempts < max_attempts:
			__message( 'URL' + ( '(' + str( attempts + 1 ) + '/' + str( max_attempts ) + ')' if attempts > 0 else '' ) + ': ' + url, debug )

			try:
				f = request.urlopen( url )

				response = f.read()

				with open( cache_file, 'w' ) as f:
					f.write( response.decode( 'utf-8' ) )

				break
			except Exception as e:
				time.sleep( 1 )

				attempts += 1
	else:
		__message( 'Retrieving from cache for URL: ' + url, debug )
		
	if (
		response is None
		and not ignore_failed_calls
	):
		raise Exception( 'Error calling URL ' + url )

	return response
