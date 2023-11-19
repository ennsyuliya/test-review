-- При наличии только одного параметра, этот параметр пишется на строке выполнения
create procedure syn.usp_ImportFileCustomerSeasonal @ID_Record int AS                                        
set 
nocount on
begin
	-- нет коментария к объявленным переменным
	declare
		/*
			 Для объявления переменных declare используется один раз. Дальнейшее
			 переменные перечисляются через запятую с новой строки, если явно не требуется
			 писать declare
		*/
		@RowCount int = (
			-- Разделение поля в объявлении переменных с новой строки
			select 
				count(*) 
			from syn.SA_CustomerSeasonal
		),
		@ErrorMessage varchar(max)


    -- Проверка на корректность загрузки
    -- Комментарий с таким же отступом как и код, к которому он относится
	if not exists (
        -- В условных операторах весь блок смещается на 1 отступ
        select 1
	    from syn.ImportFile as f
	    where f.ID = @ID_Record
		    and f.FlagLoaded = cast(1 as bit)
	    )
	-- На одном уровне с `if` и `begin/end`
    begin
		set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

		raiserror(@ErrorMessage, 3, 1)

        -- Пустая строка перед return
		return
	end

	-- Чтение из слоя временных данных
    -- Между -- и комментарием есть один пробел
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and cd.ID_mapping_DataSource = 1
		-- Сперва указываем поле присоединяемой таблицы
        join syn.CustomerSystemType as cst on cst.CustomerSystemType = cs.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	/*
		Определяем некорректные записи
		Добавляем причину, по которой запись считается некорректной
		*/
	--Для комментариев в несколько строк используется конструкция /*
	select
		cs.*
		,case
			when c.ID is null 
                -- Результат на 1 отступ от when
                then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null 
                then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null 
                then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null 
                then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null 
                then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null 
                then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null 
                then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
		-- Все виды join пишутся с 1 отступом
		left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor 
			--  Если есть and , то выравнивать его на 1 табуляцию от join
			and c_dist.ID_mapping_DataSource = 1
		left join dbo.Season as s on s.Name = cs.Season
		left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (
            -- Содержимое скобок переносится на следующую строку
            -- Перечисление атрибутов с новой строки
            ID_dbo_Customer, 
            ID_CustomerSystemType, 
            ID_Season, DateBegin, 
            DateEnd, 
            ID_dbo_CustomerDistributor, 
            FlagActive
            )
		values (
            -- Перечисление атрибутов с новой строки
            s.ID_dbo_Customer, 
            s.ID_CustomerSystemType, 
            s.ID_Season, 
            s.DateBegin, 
            s.DateEnd, 
            s.ID_dbo_CustomerDistributor, 
            s.FlagActive
            )
	;

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(
				-- Содержимое скобок переносится на следующую строку
				format(
					-- Перечисление атрибутов с новой строки
					try_cast(DateBegin as date), 
					'dd.MM.yyyy', 
					'ru-RU'
					-- Закрывающая скобка выносится на отдельную строку, если содержимое занимает несколько строк
					),
				DateBegin
			-- Алиас задается без переносов
			) as 'Дата начала'
			,isnull(
				format(
					try_cast(DateEnd as date), 
					'dd.MM.yyyy',
					'ru-RU'
					), 
				DateEnd
			) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end

end
