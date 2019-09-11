from tableaudocumentapi import Workbook
import logging
import os

logger = logging.getLogger(__name__)


def initial(filename):
    wb = Workbook(os.path.join('twbx', filename))
    data = []

    for datasource in wb.datasources:
        fields = []
        for field in datasource.fields.values():
            fields.append({
                'name': field.name,
                'role': field.role,
                'datatype': field.datatype,
                # 'Number Type': field.type,
                'caption': field.caption,
                'calculation': field.calculation == '1',
            })
        data.append({
            'name': datasource.name,
            'caption': datasource.caption or datasource.name,
            'fields': fields
        })
    return data
