import logging
import os
import os.path
import urllib.request, urllib.parse, urllib.error

from edge.writer.solrtemplateresponsewriter import SolrTemplateResponseWriter
from edge.response.solrjsontemplateresponse import SolrJsonTemplateResponse

class Writer(SolrTemplateResponseWriter):
    def __init__(self, configFilePath):
        super(Writer, self).__init__(configFilePath)
        
        self.contentType = 'application/json'

        templatePath = os.path.dirname(configFilePath) + os.sep
        templatePath += self._configuration.get('service', 'template')
        self.template = self._readTemplate(templatePath)

    def _generateOpenSearchResponse(self, solrResponse, searchText, searchUrl, searchParams, pretty):
        response = SolrJsonTemplateResponse(searchUrl, searchParams)
        response.setTemplate(self.template)

        return response.generate(solrResponse, pretty=pretty)

    def _constructSolrQuery(self, startIndex, entriesPerPage, parameters, facets):
        # if no QC flag is given, default to only good
        if not "qualityFlag" in list(parameters.keys()):
            parameters['qualityFlag'] = 1

        queries = []
        filterQueries = []
        sort = None
        min_depth = None
        max_depth = None
        include_missing_depth = False

        for key, value in parameters.items():
            if value != "":
                if key == 'keyword':
                    queries.append(urllib.parse.quote(value))
                elif key == 'startTime':
                    filterQueries.append('time:['+value+'%20TO%20*]')
                elif key == 'endTime':
                    filterQueries.append('time:[*%20TO%20'+value+']')
                elif key == 'bbox':
                    coordinates = value.split(",")
                    filterQueries.append('loc:[' + coordinates[1] + ',' + coordinates[0] + '%20TO%20' + coordinates[3] + ',' + coordinates[2] + ']')
                elif key == 'variable':
                    filterQueries.append('{}:[*%20TO%20*]'.format(value.lower()))
                elif key == "minDepth":
                    min_depth = value
                elif key == "maxDepth":
                    max_depth = value
                # include data only at specified quality level and have default at good in UI
                elif key == "qualityFlag":
                    if 'variable' in parameters:
                        variable = parameters['variable'].lower()
                        if type(value) is list:
                            filterQueries.append('({}_quality:('.format(variable) + '+OR+'.join(value) + '))')
                        else:
                            filterQueries.append('({}_quality:('.format(variable) + str(value) + '))')
                elif key == 'platform':
                    if type(value) is list:
                        filterQueries.append('pcode:(' + '+OR+'.join(value) + ')')
                    else:
                        filterQueries.append('pcode:'+value)

        if min_depth is not None and max_depth is not None and float(min_depth) <= 0 <= float(max_depth):
            include_missing_depth = True
        elif min_depth is not None and float(min_depth) <= 0:
            include_missing_depth = True
        elif max_depth is not None and 0 <= float(max_depth):
            include_missing_depth = True

        if min_depth is not None:
            if include_missing_depth:
                filterQueries.append('(depth:['+min_depth+'%20TO%20*]+OR+depth:[-99999.9%20TO%20-99998.1])')
            else:
                filterQueries.append('(depth:['+min_depth+'%20TO%20*]+AND+-depth:[-99999.9%20TO%20-99998.1])')
        if max_depth is not None:
            if include_missing_depth:
                filterQueries.append('(depth:[*%20TO%20' + max_depth + ']+OR+depth:[-99999.9%20TO%20-99998.1])')
            else:
                filterQueries.append('(depth:[*%20TO%20' + max_depth + ']+AND+-depth:[-99999.9%20TO%20-99998.1])')

        if len(queries) == 0:
            queries.append('*:*')

        query = 'q='+'+AND+'.join(queries)+'&wt=json&start='+str(startIndex)+'&rows='+str(entriesPerPage)

        if len(filterQueries) > 0:
            query += '&fq='+'+AND+'.join(filterQueries)

        if sort is not None:
            query += '&sort=' + sort

        if 'stats' in parameters and parameters['stats'].lower() == 'true':
            query += '&stats=true&stats.field={!min=true%20max=true}sss_depth&stats.field={!min=true%20max=true}sst_depth&stats.field={!min=true%20max=true}wind_depth'

        if 'facet' in parameters and parameters['facet'].lower() == 'true':
            query += '&facet=true&facet.field=pcode&facet.field=device&facet.limit=-1&facet.mincount=1'

        logging.debug('solr query: '+query)

        return query
