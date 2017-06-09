#!/bin/bash

USER=dbadmin
PWD=
DATABASE=vmart
HOST=localhost

vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./promotion_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./store_sales_fact.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./online_page_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./customer_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./online_sales_fact.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./vendor_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./shipping_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./inventory_fact.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./store_orders_fact.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./store_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./call_center_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./employee_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./date_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./warehouse_dimension.sql
vsql -d ${DATABASE} -h ${HOST} -w ${PWD} -u ${USER} -f ./product_dimension.sql
