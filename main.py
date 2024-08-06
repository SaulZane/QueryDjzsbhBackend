import pandas as pd
import polars as pl
import duckdb
from fastapi import FastAPI,File,UploadFile,Form,Body,WebSocket,HTTPException
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import uvicorn
from sqlmodel import Field, Session, SQLModel, create_engine, select, update,func
import os
from fastapi.responses import FileResponse
import traceback

#重启oracle服务监听,数据库信息
#lsnrctl status   identified by trff_app
#uvicorn main:app  --port 8002 --reload

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


engine=create_engine("oracle+oracledb://veh_admin:veh_admin@192.168.1.116:1521/?service_name=orcl",echo=True)


@app.get("/")
def root():
    return {"服务器已启动。"}

@app.get("/test")
def test(hphm: str,hpzl: str):
    """
    根据车牌号和车型查询车辆信息。

    参数:
    hphm (str): 车牌号。
    hpzl (str): 车型。

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
            result = Vehicle(hpzl=hpzl, hphm=hphm, djzsbh="")
        # 返回查询结果或新创建的Vehicle对象
        return result

@app.post("/excel")
def create_upload_file(file: UploadFile = File(...)):
    """
    上传并处理Excel文件。

    该函数首先检查上传的文件是否为Excel格式，若不是，则返回错误信息。
    随后，它读取Excel文件内容，将特定列转换为字符串类型，并遍历每一行数据，
    通过调用`test`函数处理车牌号和车辆类型，以获取对应的`djzsbh`编号。
    最后，根据处理结果更新Excel文件，并返回处理后的文件。

    参数:
    - file: 上传的文件，类型为UploadFile，必须通过File(...)依赖注入。

    返回:
    - 若处理成功，返回包含处理结果的Excel文件，文件名为"查询结果.xlsx"。
    - 若处理失败，返回包含Error的堆栈信息。
    """
    try:
        # 判断是否为excel文件，如果有不是，反馈错误
        if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return {"error": "只能上传Excel文件"}
        # 读取excel文件为DataFrame，并将特定列转换为字符串类型
        df = pd.read_excel(file.file)
        df["后六位"] = df["后六位"].astype(str)
        # 遍历每行数据，处理hpzl和hphm,然后通过test函数找到djzsbh，更新到vehicle类中
        for index, row in df.iterrows():
            hphm = row["车牌号"]
            hpzl = hpzlType[row["车辆类型"]]
            vehicle = test(hphm, hpzl)
            # 根据vehicle的djzsbh值，更新DataFrame对应行的“后六位”列
            if vehicle.djzsbh == "":
                df.at[index, "后六位"] = "!!基础数据错误,数据无效!!"
            elif vehicle.djzsbh is None:
                df.at[index, "后六位"] = "无编号"
            else:
                df.at[index, "后六位"] = "*" + str(vehicle.djzsbh[-6:])
        # 打印处理后的DataFrame
        print(df)
        # 将处理结果保存到Excel文件，并返回该文件
        df.to_excel("./test.xlsx", index=False)
        return FileResponse("./test.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename="查询结果.xlsx")
    except Exception as e:
        # 异常处理：返回错误信息和堆栈跟踪
        return {"error": str(e) + "\n" + traceback.format_exc()}






    

