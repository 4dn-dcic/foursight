from dcicutils import ff_utils

def calculate_qc_metric_pairsqc(file_uuid, key):
    '''Patching a pairs file object with quality_metric summary'''
    res = ff_utils.get_metadata(file_uuid, key=key)
    qc_uuid = res['quality_metric']['uuid']
    quality_metric = ff_utils.get_metadata(qc_uuid, key=key)
    qc_summary = []
    def percent(numVal):
        '''convert to percentage of Total reads'''
        return round((numVal / quality_metric['Total reads']) * 100 * 1000) / 1000
    def million(numVal):
        return str(round(numVal / 10000) / 100) + "m"
    def tooltip(numVal):
        return "Percent of total reads (=%s)" % million(numVal)
    qc_summary.append({"title": "Filtered Reads",
                                          "value": str(quality_metric["Total reads"]),
                                          "numberType": "integer"}) 
    qc_summary.append({"title": "Cis reads (>20kb)",
                                          "value": str(percent(quality_metric["Cis reads (>20kb)"])),
                                          "tooltip": tooltip(quality_metric["Cis reads (>20kb)"]),
                                          "numberType": "percent"})
    qc_summary.append({"title": "Short cis reads",
                                          "value": str(percent(quality_metric["Short cis reads (<20kb)"])),
                                          "tooltip": tooltip(quality_metric["Short cis reads (<20kb)"]),
                                          "numberType": "percent"})
    qc_summary.append({"title": "Trans Reads",
                                          "value": str(percent(quality_metric["Trans reads"])),
                                          "tooltip": tooltip(quality_metric["Trans reads"]),
                                          "numberType": "percent"})
    res = ff_utils.patch_metadata({'quality_metric_summary': qc_summary}, file_uuid, key=key)
    return res

    
def parse_formatstr(file_format_str):
    if not file_format_str:
        return None
    return file_format_str.replace('/file-formats/', '').replace('/', '')
