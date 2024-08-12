#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : main.py
@Time    : 2024/8/6-8/7
@Author  : 张硕
@Description：处理excel的主文件
"""

import pandas as pd
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Field, Session, SQLModel, create_engine, select
from fastapi.responses import FileResponse
import traceback
import os

#重启oracle服务监听,数据库信息
#lsnrctl status   identified by trff_app
#uvicorn main:app  --port 8002 --reload
from sqlalchemy import create_engine

#https://www.oracle.com/database/technologies/appdev/python/quickstartpythononprem.html
#https://docs.sqlalchemy.org/en/20/dialects/oracle.html#module-sqlalchemy.dialects.oracle.oracledb
app = FastAPI()


# 添加CORS中间件，允许所有来源、所有方法和所有头
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源
    allow_credentials=True,  # 允许凭证，如cookies
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有请求头
)

class Vehicle(SQLModel, table=True):
    hpzl: str 
    hphm: str = Field(primary_key=True)
    djzsbh: str


hpzlType={
    "大型汽车":"01",
    "小型汽车":"02",
    "使馆汽车":"03",
    "领馆汽车":"04",
    "境外汽车":"05",
    "外籍汽车":"06",
    "普通摩托车":"07",
    "轻便摩托车":"08",
    "使馆摩托车":"09",
    "领馆摩托车":"10",
    "境外摩托车":"11",
    "外籍摩托车":"12",
    "低速车":"13",
    "拖拉机":"14",
    "挂车":"15",
    "教练汽车":"16",
    "教练摩托车":"17",
    "试验汽车":"18",
    "试验摩托车":"19",
    "临时入境汽车":"20",
    "临时入境摩托车":"21",
    "临时行驶车":"22",
    "警用汽车":"23",
    "警用摩托":"24",
    "原农机号牌":"25",
    "香港入境车":"26",
    "澳门入境车":"27",
    "武警号牌":"31",
    "军队号牌":"32",
    "应急号牌":"33",
    "无号牌":"41",
    "假号牌":"42",
    "挪用号牌":"43",
    "大型新能源汽车":"51",
    "小型新能源汽车":"52",
    "其它号牌":"99",
}

#这个是兼容字符串，必须安装cx_oracle才能运行，安装whl包https://blog.csdn.net/weixin_44100044/article/details/126034475
engine=create_engine("oracle+cx_oracle://veh_admin:veh_admin@192.168.1.116:1521/?service_name=orcl",echo=True)



@app.get("/")
def root():
    return {"服务器已启动。"}

totalprocess:int =0#要处理的总数
process:int=0#正在处理的行号

@app.get("/test")
def test(hphm: str,hpzl: str):
    """
    根据车牌号和车型查询车辆信息。

    参数:
    hphm (str): 车牌号。
    hpzl (str): 号牌种类代码值。

    返回:
    Vehicle: 查询到的车辆信息对象，如果未查询到，则返回一个空的登记证书编号。
    """
    # 创建数据库会话
    with Session(engine) as session:
        # 构建查询语句，查询Vehicle表中车牌号(hphm)和车型(hpzl)匹配的记录
        statement = select(Vehicle).where(Vehicle.hphm == hphm).where(Vehicle.hpzl == hpzl)
        # 初始化结果为None，后续根据查询情况赋值
        result = None
        try:
            # 执行查询语句，尝试获取单个匹配的记录
            result = session.exec(statement).one()
        except Exception as e:  # 如果执行中出现异常（例如，无匹配记录），捕获异常并处理
            # 如果出现异常，创建并返回一个新的Vehicle对象，djzsbh字段设为空字符串
            result = Vehicle(hpzl=hpzl, hphm=hphm, djzsbh=str(e))
        # 返回查询结果或新创建的Vehicle对象
        return result

@app.post("/excel")
async def create_upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    上传并处理Excel文件。

    该函数首先检查上传的文件是否为Excel格式，若不是，则返回错误信息。并查看是否满足模板设置。
    随后，它读取Excel文件内容，将特定列转换为字符串类型。
    然后，它遍历每一行数据，生成一个后台任务来处理hpzl和hphm。
    通过调用`test`函数，它可以获取对应的`djzsbh`编号。
    最后，它更新Excel文件，并返回处理后的文件。

    参数:
    - file: 上传的文件，类型为UploadFile，必须通过File(...)依赖注入。

    返回:
    - 若处理成功，返回包含处理结果的Excel文件，文件名为"查询结果.xlsx"。
    - 若处理失败，返回包含Error的堆栈信息。
    """
    try:
        # 检查上传的文件是否为Excel文件，若不是，则返回错误信息。
        if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return {"error": "只能上传Excel文件"}

        # # 读取Excel文件内容，并将特定列转换为字符串类型。
        # df = pd.read_excel(file.file)

        spooled_tmp_file = file.file  # 这里 file.file 是 SpooledTemporaryFile 对象
        file_as_bytes = spooled_tmp_file.read()
        df = pd.read_excel(file_as_bytes)
        # 检查Dataframe是否具有所需的列，若不是，则返回错误信息。
        if "后六位" not in df.columns:
            return {"error": "模板错误，缺少'后六位'列"}
        df["后六位"] = df["后六位"].astype(str)

        if "车牌号" not in df.columns:
            return {"error": "模板错误，缺少'车牌号'列"}
        df["车牌号"] = df["车牌号"].astype(str)

        if "车辆类型" not in df.columns:
            return {"error": "模板错误，缺少'车辆类型'列"}

        # 获取总行数
        global totalprocess
        totalprocess = df.shape[0]

        # 遍历每一行数据，生成一个后台任务来处理hpzl和hphm，并通过test函数获取对应的djzsbh。
        answer: FileResponse = background_tasks.add_task(task, df)
        return answer

    except Exception as e:
        # 异常处理：返回错误信息和堆栈跟踪。
        return {"error": str(e) + "\n" + traceback.format_exc()}
@app.post("/excel")
async def create_upload_file(background_tasks: BackgroundTasks,file: UploadFile = File(...)):
    """
    上传并处理Excel文件。

    该函数首先检查上传的文件是否为Excel格式，若不是，则返回错误信息。并查看是满足模板设置
    随后，它读取Excel文件内容，将特定列转换为字符串类型，并遍历每一行数据，（生成一个task后台任务）
    通过调用`test`函数处理车牌号和车辆类型，以获取对应的`djzsbh`编号。
    最后，根据处理结果更新Excel文件，并返回处理后的文件。

    参数:
    - file: 上传的文件，类型为UploadFile，必须通过File(...)依赖注入。

    返回:
    - 若处理失败，返回包含Error的堆栈信息。
    """
    try:
        # 判断是否为excel文件，如果有不是，反馈错误
        if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return {"error": "只能上传Excel文件"}
        # 读取excel文件为DataFrame，并将特定列转换为字符串类型
        df = pd.read_excel(file.file)
        if "后六位" not in df.columns:
            return {"error": "模板错误"}
        df["后六位"] = df["后六位"].astype(str)
        if "车牌号" not in df.columns:
            return {"error": "模板错误"}
        df["车牌号"] = df["车牌号"].astype(str)
        if "车辆类型" not in df.columns:
            return {"error": "模板错误"}
        
        df["后六位"] = df["后六位"].astype(str)

        # 获取总行数
        global totalprocess
        totalprocess = df.shape[0]
        # 遍历每行数据，处理hpzl和hphm,然后通过test函数找到djzsbh，更新到vehicle类中
        answer:FileResponse=background_tasks.add_task(task, df)
        return answer
    
    except Exception as e:
        # 异常处理：返回错误信息和堆栈跟踪
        return {"error": str(e) + "\n" + traceback.format_exc()}

def task(df: pd.DataFrame) -> FileResponse:
    """
    这个函数通过遍历给定的DataFrame，对每一行调用test函数来获取djzsbh值，并更新对应的列。

    参数:
        df (pd.DataFrame): 要处理的DataFrame。

    返回:
        FileResponse: 处理后的DataFrame保存为Excel文件。
    """
    # 删除旧的test.xlsx文件（如果存在）
    if os.path.exists("./test.xlsx"):
        os.remove("./test.xlsx")

    # 遍历DataFrame的每一行
    for index, row in df.iterrows():
        global process
        process = index + 1  # 更新处理计数
        hphm = row["车牌号"]  # 获取车辆识别号
        hpzl = hpzlType[row["车辆类型"]]  # 获取车辆类型
        vehicle = test(hphm, hpzl)  # 调用test函数获取djzsbh

        # 根据djzsbh值更新DataFrame对应行的“后六位”列
        if vehicle.djzsbh == "":
            df.at[index, "后六位"] = "!!基础数据错误,数据无效!!"
        elif vehicle.djzsbh is None:
            df.at[index, "后六位"] = "无编号"
        else:
            df.at[index, "后六位"] = "*" + str(vehicle.djzsbh[-6:])

    # 保存处理结果到Excel文件并返回该文件
    df.to_excel("./test.xlsx", index=False)
    return FileResponse("./test.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="查询结果.xlsx")
@app.get("/process")
async def process():
    """
    这个函数返回Excel文件的处理进度。

    如果处理已完成，则返回处理后的Excel文件。
    如果处理还未完成，则返回当前处理进度和总处理量。

    返回:
        FileResponse or dict: 处理后的Excel文件或处理进度信息。
    """
    # 检查处理是否已完成
    if process == totalprocess and totalprocess != 0:
        # 返回处理后的Excel文件
        return FileResponse(
            "./test.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename="查询结果.xlsx"
        )
    else:
        # 返回当前处理进度和总处理量
        return {
            "process": process,
            "totaltoprocess": totalprocess
        }


@app.get("/example")
async def example():
    """
    这个函数返回一个模板的Excel文件。

    返回:
        FileResponse: 模板的Excel文件。
    """

    # 返回一个模板的Excel文件
    # 该文件的内容类型为"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"，
    # 文件名为"模板.xlsx"
    return FileResponse(
        "./模板.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="模板.xlsx"
    )
@app.get("/finish") 
async def finish():
    """
    这个函数已经查询完成好的Excel文件。

    返回:
    -FileResponse: 已经查询完成好的Excel文件。
    """
    return FileResponse("./test.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="查询结果.xlsx")