"""
-------------------------------------------------------
Loads Entire Working Database
-------------------------------------------------------
Author:  JD
ID:      #
Email:   jsingh@live.com
__updated__ = "2023-05-27"
-------------------------------------------------------
"""
# Imports
from functions import DATAFILES, WEBFILES, clocked_in, CBBLIVE
from json import loads
from linked_sortedList import SortedList
from linked_queue import Queue
from orderLine import orderLine
from operation import inWork
from timeticket import timeTicket
from datetime import datetime
from dateutil.parser import parse
from pandas import read_excel
from database_po import PODatabase


# Constants


class Database:
    def __init__(self):
        self._database = loads(
            open(f'{DATAFILES}livedata.json', 'r', encoding='utf-8').read())
        self._count = 0
        self.openorders = SortedList()
        self.TotalDollarAmount=0
        self.conversionFactor=.76
        for item in self._database:
            self._database[item] = orderLine(self._database[item])
            self._count += 1
            if self._database[item].status == 'Open':
                self.openorders.append(
                    self._database[item], self._database[item].dueDate)
        
        self.timeTicketsDatabase=SortedList()
        self.__update_time_tickets()
        self.update(read_excel(CBBLIVE).values.tolist()[4:])
        self.in_work()
        self.actualopencount = 0
        self.openactual = self._openact()
        self.machineSDH = {}

        self.PODatabase=PODatabase()

    def workOrder(self, wo):
        value = None
        if wo in self._database:
            value = self._database[wo]
        return value

    def wo_list(self):
        return list(self._database.keys())

    def count(self, tag=None):
        count = self._count
        if tag == 'OPEN':
            count = self.openorders._count
        return count



    def _updateTimeTicketDatabase(self,ticket):
        if ticket.empCode in self.timeTicketsDatabase:
            self.timeTicketsDatabase[ticket.empCode].append(ticket,ticket.ticketDate)
        else:
            self.timeTicketsDatabase[ticket.empCode]=SortedList()
            self.timeTicketsDatabase[ticket.empCode].append(ticket,ticket.ticketDate)
        return None
    
    def __update_time_tickets(self):
        self._data = loads(
            open(f'{DATAFILES}timeTicket.json', 'r', encoding='utf-8').read())
        for a in self._data:
            if a in self._database and self._database[a].router is not None:
                for each in self._data[a]:
                    ticket=timeTicket(each)
                    for op in self._database[a].router:
                        if op.stepNumber == each['stepNumber']:
                            op.timeTickets.append(
                                ticket, each['ticketDate'])
                    self.timeTicketsDatabase.append(ticket,ticket.ticketDate)
        return None           

    def __dropped_wo(self):
        fh = open(f'{DATAFILES}dropped.txt', 'r', encoding='utf-8')
        orders = []
        for each in fh:
            orders.append(each.strip())
        fh.close()
        fh = open(f'{DATAFILES}completed.txt', 'r', encoding='utf-8')
        for each in fh:
            orders.append(each.strip())
        fh.close()
        return orders

    def _openact(self):
        openorders = SortedList()
        orders = self.__dropped_wo()
        for order in self.openorders:
            if order.jobNumber not in orders:
                openorders.append(order, order.dueDate)
                self.actualopencount += 1
        return openorders

    def monthlyBreakdown(self):
        monthly = {}
        for wo in self.openactual:
            if wo.jobNumber not in self.__dropped_wo():

                if f'{wo.dueDate.month}-{wo.dueDate.year}' in monthly:
                    monthly[f'{wo.dueDate.month}-{wo.dueDate.year}'].append(
                        wo, wo.dueDate)
                else:
                    monthly[f'{wo.dueDate.month}-{wo.dueDate.year}'] = SortedList()
                    monthly[f'{wo.dueDate.month}-{wo.dueDate.year}'].append(
                        wo, wo.dueDate)
        return monthly

    def TABreakDown(self):
        data = {}
        for wo in self.openactual:
            if wo.salesID in data:
                data[wo.salesID].append(wo, wo.dueDate)
            else:
                data[wo.salesID] = SortedList()
                data[wo.salesID].append(wo,wo.dueDate)
        return data

    def liveDataFile(self):
        fh = open(f'{DATAFILES}livedatafile1.csv', 'w', encoding='utf-8')
        for wo in self.openactual:
            desciption = wo.des.replace('\n', '')
            wrt = f'{wo.jobNumber},{wo.lastModDate},{wo.customerCode},{wo.customerName},{wo.PONumber},{wo.salesID},{wo.currencyCode},{wo.partNumber},{wo.quantityOrdered},{wo.quantityToStock},{wo.pricingUnit},{desciption},{wo.unitPrice},{wo.dueDate},{wo.productCode},{wo.PR},'
            for op in wo.router:
                name = op.workCenter
                if op.workCenter is None:
                    name = op.vendorCode
                # op.des.replace('\n','||').replace(',',' ').strip()
                desciption = 1
                wrt += f'{name}|||{desciption} || ||Setup Time:{op.setupTime} {op.setupTimeUnit} || CycleTime:{op.cycleTime} {op.cycleTimeUnit},'
            fh.write(f'{wrt}\n')

    def update(self, ds):
        def shp(txt):
            if type(txt)==float:
                txt=''
            return str(txt)
        
        def checkper(txt):
            if type(txt) is float:
                txt=str(round(a[5]*100, 2))
            return txt
        
        ds = ds[4:]
        k, self.ordersOnLive = 0, 0
        for a in ds:
            if a[0] in self._database:
                self.ordersOnLive += 1
                wo = self._database[a[0]]
                # if a[3].upper() == 'HOLD':
                # wo.dueDate = datetime.strptime('15 May, 2020', "%d %B, %Y")
                # else:
                wo.dueDate = parse(str(a[3]))
                wo.completed = checkper(a[5])
                wo.notes = shp((a[8]))
                wo.shipping = shp(a[6])
                wo.incoming = shp(a[9])
                wo.ME = shp(a[10])
                wo.PR = shp(a[11])

                i,r=0,a[12:57]
                while i<len(r) and type(r[i]) is float:
                    i+=1
                
                k=0
                r=wo.router._front
                while k<i and r is not None:
                    r._value.status='DONE'
                    r=r._next
                    k+=1
                   

        return None

    def in_work(self):
        for each in clocked_in():
            if each[2] in self._database:
                for op in self._database[each[2]].router:
                    self._database[each[2]].inwork='In-Work'
                    if op.stepNumber == each[6]:
                        op.status = 'In-Work'
                        op.inworkDet = inWork(
                            each[0], each[1], each[9], each[11])
        return None

    def _update_scd(self, op, date):
        if op.workCenter in self.machineSDH:
            if self.machineSDH[op.workCenter]._count < 10:
                self.machineSDH[op.workCenter].append(op, date)
        else:
            self.machineSDH.update({op.workCenter: SortedList()})
            self.machineSDH[op.workCenter].append(op, date)
        return None

    def updateMachineSchedule(self):
        info = open('templates/MachineInfo/machineschd.html',
                    'r', encoding='utf-8').read()
        for workCenter in self.machineSDH.values():
            curr = workCenter._front
            prev = None
            i = 1
            in_ = ''
            while curr is not None:
                wo = self._database[curr._value.jobNumber]
                in_ += f'''<tr class="datarow">
                            <td>{i}</td>
                            <td><a href="{curr._value.jobNumber}.html">{curr._value.jobNumber}</a></td>
                            <td>{wo.customerName}</td>
                            <td>{curr._value.stepNumber}</td>
                            <td>{curr._value.des}</td>
                        </tr>'''
                prev = curr
                curr = curr._next
                i += 1

            k = prev._value.workCenter.replace('/', '-')
            ins = info.replace('**INFO**', in_)
            ins = ins.replace('**MACHINE**', k)
            ins = ins.replace('**TIME**', str(datetime.today())
                              [:20].replace(' ', '&emsp;'))

            open(f'{WEBFILES}workorderpages/0{k}.html',
                 'w', encoding='utf-8').write(ins)
        # open(f'{WEBFILES}workorderpages/machineinfo.css','w',encoding='utf-8').write(open(f'templates/MachineInfo/machineinfo.css','r', encoding='utf-8').read())

    def createInfoPage(self):

        in_ = open(f'templates/WOInfo/WorkOrderInfo.html', 'r',
                   encoding='utf-8').read()  # template file
        autofill_srt = ''
        number = 0

        curr = self.openactual._front
        prev = None
        while curr is not None:
            wo = curr._value
            operation_que = Queue()
            autofill_srt += f'"{wo.jobNumber}",\n'
            info, op_str, quote_hr_str, act_hr_str, clrs = '', '', '', '', ''

            
            if wo.unitPrice is not None:
                wo.value=wo.unitPrice*wo.quantityOrdered
            
            self.TotalDollarAmount+=wo.value
            
            wo.dueIN = (wo.dueDate - datetime.today()).days
            if wo.dueIN >= 7:
                due_in = f'<td style="color: green;">{wo.dueIN}</td>'
                wo.tag = 'on-time'
            elif wo.dueIN < 7 and wo.dueIN >= 0:
                due_in = f'<td style="color: gold;">{wo.dueIN}</td>'
                wo.tag = 'critical'
            else:
                due_in = f'<td style="background-color: ; color: red;">{wo.dueIN}</td>'
                wo.tag = 'late'

            # Quantity
            
            wo.qty_str = f'{wo.quantityOrdered}'
            if wo.quantityToStock >= 1:
                wo.qty_str = f'{wo.quantityOrdered}+{wo.quantityToStock}'

            total, pending = 0, 0
            for op in wo.router:
                number, hours, ho = 0, 0, 0
                # Work Center Name-if None Vendor Code
                if op.workCenter is None:
                    op.workCenter = op.vendorCode

                # Fix Operation Description
                op.des = str(op.des).replace('"', "''")

    

                checklist, comp = '', ''
                if op.des is not None:
                    if 'F-133' in op.des.upper():
                        checklist = '<img src="checklist.png" style="width: 20px;" alt="Fill F-133"></i>'

                codes, names, hours_, dates_ = '', '', '', ''
                for t in op.timeTickets:
                    if t.cycleTime is None:
                        t.cycleTime = 0
                    codes += f'{t.empCode}<br>'
                    names += f'{t.empname}<br>'
                    hours_ += f'{round(t.cycleTime,2)} H<br>'
                    hours += t.cycleTime
                    dates_ += f'{t.ticketDate.date()}<br>'

                op.jobNumber = wo.jobNumber
                total += ho
                if op.status == 'In-Work':
                    codes += f'<p id="inwork" title="In Work">{op.inworkDet.code}</p>'
                    names += f'<p id="inwork" title="In Work">{op.inworkDet.emp}</p>'
                    hours_ += f'<p id="inwork" title="In Work">{op.inworkDet.time}</p>'
                    dates_ += f'<p id="inwork" title="In Work">Status:{op.inworkDet.status}</p>'
                    pending += ho
                    operation_que.insert(op)

                elif op.status == 'PENDING':
                    pending += ho
                    operation_que.insert(op)

                info += f'''<tr class="datarow" id="{op.status}">
                <td>{op.stepNumber}</td>
                <td title="{op.des}"><a href="0{op.workCenter}.html">{op.workCenter}</a></td>
                <td class="f133"> {checklist} </td>
                <td class="act" style="border-right:solid black 1px; text-align: center;"> Setup+Cycle: {round(op.totalEstimatedHours,2)} H</td>
                <td class="act" style="padding-left:15px;">{codes}</td>
                <td class="act" style="padding-left:10px;">{names}</td>
                <td class="act">{hours_}</td>
                <td class="act" style="border-right:solid black 1px;">{dates_}</td>
                <td class="act" style="padding-left:15px;">{round(hours,2)} Hrs</td>
            
                </tr>
                '''
        
                act_hr_str += f'{hours},'
                quote_hr_str += f'{op.totalEstimatedHours},'
                op_str += f'"{op.workCenter}",'
                if op.totalEstimatedHours >= hours:
                    clrs += '"green",'
                else:
                    clrs += '"red",'

            j, b = 0, wo.dueDate
            if wo.PR == '1':
                b = datetime.today()
            
            
               
            while operation_que.is_empty() is False and j < 2:
                self._update_scd(operation_que.remove(), b)
                j += 1

            info = in_.replace('**INFO**', info)
            info = info.replace('**DES**', wo.des)
            info = info.replace('**WO**', wo.jobNumber)
            info = info.replace('**WO**', wo.jobNumber)
            info = info.replace('**WO**', wo.jobNumber)
            info = info.replace('**QTY**', wo.qty_str)
            info = info.replace('**DUE**', wo.dueDate.strftime("%d-%B-%Y"))
            info = info.replace('**CUSTOMER**', str(wo.customerName))
            info = info.replace('**DUEIN**', due_in)
            info = info.replace('**COMP**', str(wo.completed))
            info = info.replace('**SHP**', wo.shipping)
            info = info.replace('**TA**', str(wo.salesID))
            info = info.replace('**NOTES**', wo.notes)
            info = info.replace('**ME**', wo.ME)
            info = info.replace('**PR**', wo.PR)
            info = info.replace('**OPS**', op_str)
            info = info.replace('**ATC**', act_hr_str)
            info = info.replace('**QTC**', quote_hr_str)
            info = info.replace('**CRL**', clrs)
            info = info.replace(
                '**CM**', f'{wo.completed},{100-float(wo.completed)}')
            info = info.replace(
                '**TIME**', str(datetime.today())[:20].replace(' ', '&emsp;'))

            if curr._next is None:
                info = info.replace(
                    '**NEXT**',  self.openactual._front._value.jobNumber)
            else:
                info = info.replace('**NEXT**',  curr._next._value.jobNumber)
            if prev is None:
                info = info.replace(
                    '**PREV**', self.openactual._rear._value.jobNumber)
            else:
                info = info.replace('**PREV**', prev._value.jobNumber)
            # Create WebPage
            open(f'{WEBFILES}workorderpages/{wo.jobNumber}.html',
                 'w', encoding='utf-8').write(info)
            number += 1
            prev = curr
            curr = curr._next

        autofill_srt += '"d"'
        auto_fh = open(f'templates/autocomplete.txt', 'r',
                       encoding='utf-8').read().replace('**wo**', autofill_srt)
        open(f'{WEBFILES}/autocomplete.js', 'w',
             encoding='utf-8').write(auto_fh)
        open(f'{WEBFILES}workorderpages/workorder.css', 'w', encoding='utf-8').write(
            open(f'templates/WOInfo/workorder.css', 'r', encoding='utf-8').read())
        return None

    def updatesheduleBoard(self):
        info = ''
        workCenterList = SortedList()
        for workCenter in self.machineSDH.values():
            curr = workCenter._front
            info += f'''<div class="swim-lane">
                    <div class="mach-head"><a href="workorderpages/0{curr._value.workCenter}.html"><h3 class="work-center-name" >{curr._value.workCenter}</h3></a></div>
            '''
            workCenterList.append(curr._value.workCenter,
                                  curr._value.workCenter)
            i = 0
            while i < len(workCenter) and i < 5:
                wo = self._database[curr._value.jobNumber]
                info += f'<div class="task" draggable="true" id="{curr._value.status}"><a href="workorderpages/{wo.jobNumber}.html"><p class="cust-name">{str(wo.customerName)[:16]}</p><p class="work-order">{wo.jobNumber}&emsp;OP{curr._value.stepNumber}</p> <p class="des">{str(wo.des)[:40]}</p><p class="due">Due On: {wo.dueDate.strftime("%d/%m/%Y")}&emsp;Qty: {wo.qty_str}</p> <p class="due-in" id="{wo.tag}">{wo.dueIN}</p><a></div>'
                i += 1
                curr = curr._next
            info += '</div>\n\n\n'
        open(f'{WEBFILES}board.html', 'w', encoding='utf-8').write(open('templates/SchdBoard/board.html', 'r',
                                                                        encoding='utf-8').read().replace('**INFO**', info).replace('**TIME**', str(datetime.today())[:20].replace(' ', '&emsp;')))
        open(f'{WEBFILES}styles.css', 'w', encoding='utf-8').write(
            open('templates/SchdBoard/styles.css', 'r', encoding='utf-8').read())

        info = ''
        for wc in workCenterList:
            info += f'<tr><td><a href="workorderpages/0{wc}.html">{wc}</a></td></tr>\n'



        open(f'{WEBFILES}index.html', 'w', encoding='utf-8').write(open(
            'templates/index/index.html', 'r', encoding='utf-8').read().replace('**INFO**', info).replace('**TA**',self.TApages()))
        open(f'{WEBFILES}styles-main.css', 'w', encoding='utf-8').write(
            open('templates/index/styles-main.css', 'r', encoding='utf-8').read())
        return None


    def TApages(self):
        in_=open('templates/JobTracker/jobtracker.html','r',encoding='utf-8').read()
        TA_LIST=''
        for order in self.TABreakDown().values():
            curr = order._front
            info=''
            monthly={}
            str4,str5='',''
            while curr is not None:
                
                wo=curr._value
                info+=f'''<tr class="datarow">
                        <td class="col1-head" id="{wo.inwork}" title="{wo.des}"> <a href="{wo.jobNumber}.html">{wo.jobNumber}</a></td>
                        <td id="{wo.inwork}">{wo.customerName}</td>
                        <td id="{wo.inwork}">{wo.qty_str}</td>
                        <td id="{wo.tag}">{wo.dueIN}</td>
                        <td >{wo.dueDate.strftime("%d-%B-%Y")}</td>
                        <td>{wo.notes}</td>
                        <td>{wo.completed}%</td>
                        <td>{wo.shipping}</td>
                        <td>${wo.value}</td>
                        <td>{wo.PONumber}</td>
                '''
                tag=wo.dueDate.strftime("%d %b, %Y")[3:]
                if tag in monthly:
                    monthly[tag][0]+=wo.value
                    monthly[tag][1]+=1
                else:
                    monthly[tag]=[0,0]
                    monthly[tag][0]+=wo.value
                    monthly[tag][1]+=1

                for op in wo.router:
                    info+=f'<td id="{op.status}" title="{op.stepNumber} \n{op.des}">{op.workCenter}</td>'
                info+="</tr>"
                if curr._next is not None:
                    if wo.dueDate.month!=curr._next._value.dueDate.month:
                        info+='<tr></tr>'

                str4+=f"'{wo.jobNumber}',"
                str5+=f'{wo.completed},'
                curr=curr._next

            str1,str2,str3,='','',''
            l1,l2=[],[]
            for value in monthly:
                str1+=f"'{value}',"
                str2+=f'{monthly[value][0]},'
                l1.append(monthly[value][0])
                str3+=f'{monthly[value][1]},'
                l2.append(monthly[value][1])

            

            invert=int((max(l1)-min(l1))/len(l1))
            invert2=int((max(l2)-min(l2))/len(l2))
            info=in_.replace('**TABLE**',info)
            info=info.replace('**TA**',str(wo.salesID))
            info=info.replace('**TA**',str(wo.salesID))
            info=info.replace("'**STR1**'",str1)
            info=info.replace("'**STR2**'",str2)
            info=info.replace("'**STR3**'",str3)
            info=info.replace("'**STR4**'",str4)
            info=info.replace("'**STR5**'",str5)
            info=info.replace("'*MIN*'",str(min(l1)-10000))
            info=info.replace("'*MAX*'",str(max(l1)+50000))
            info=info.replace("'*STEP*'",str(invert))
            info=info.replace("'*MIN1*'",str(min(l2)-2))
            info=info.replace("'*MAX1*'",str(max(l2)+2))
            info=info.replace("'*STEP1*'",str(invert2))
            info=info.replace("**TIME**",str(datetime.today()))

            info=info.replace('**TABLE1**',self.PODatabase.PO_live(wo.salesID))

            TA_LIST+=f'<tr><td><a href="workorderpages/{wo.salesID}.html">{wo.salesID}</a></td></tr>'
            open(f'{WEBFILES}workorderpages/{wo.salesID}.html', 'w',encoding='utf-8').write(info)
        open(f'{WEBFILES}workorderpages/jobtracker.css', 'w', encoding='utf-8').write(open(f'templates/JobTracker/jobtracker.css', 'r', encoding='utf-8').read())
        return TA_LIST
    
    def notClocketIn(self):
        clockedIn,attendence,indirect,missing=[],[],[],[]
        

        file1=read_excel(f'{DATAFILES}GridExport2.xlsx').values.tolist()
        for each in file1:
            attendence.append(each[1])

        file1=read_excel(f'{DATAFILES}GridExport1.xlsx').values.tolist()
        for each in file1:
            clockedIn.append(each[1])
            if 'INDIRECT' in each[4]:
                indirect.append(each[1])

        for each in attendence:
            if each not in clockedIn:
                missing.append(each)
    
        print(missing)
        print(indirect)
        return None
