from tableaudocumentapi import Workbook
import logging

logger = logging.getLogger(__name__)


def initial(filename):
    wb = Workbook(filename)
    data = []

    for datasource in wb.datasources:
        fields = []
        for field in datasource.fields.values():
            fields.append({
                'Name': field.name,
                'Role': field.role,
                'Type': field.datatype,
                # 'Number Type': field.type,
                'Caption': field.caption,
                'Calculation': field.calculation == '1',
            })
        data.append({
            'name': datasource.name,
            'caption': datasource.caption,
            'fields': fields
        })
    return data
