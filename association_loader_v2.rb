
class Loaders::Association < GraphQL::Batch::Loader
  attr_reader :scope, :distinct, :where, :joins, :order, :page, :per_page
  attr_reader :model, :model_table_name, :model_primary_key, :bucket, :is_collection, :use_preloader, :is_polymorphic
  attr_reader :association, :association_name, :association_model, :association_table_name, :association_primary_key, :association_foreign_key, :association_foreign_type

  def initialize(model, association_name, scope: nil, distinct: false, where: {}, joins: {}, order: {}, page: nil, per_page: nil)
    @model = model
    @model_table_name = model.table_name
    @model_primary_key = model.primary_key

    @association_name = association_name
    @association = @model.reflect_on_association(association_name)

    validate()

    @is_collection = @association.collection?
    @is_polymorphic = @association.polymorphic?
    @use_preloader = @is_polymorphic || @association.scopes.present?

    unless @is_polymorphic
      @association_table_name = @association.table_name
      @association_model = @association.klass
      @association_primary_key = @association_model.primary_key
      @association_foreign_key = @association.foreign_key
      @association_foreign_type = @association.foreign_type

      @scope = scope
      @distinct = distinct
      @joins = joins
      @where = where
      @order = order
      @page = page
      @per_page = per_page

      if @scope.nil? && (@distinct.present? || @where.present? || @joins.present? || @order.present?)
        @scope = add_scope(@association_model)
      end
    end

    validate_pagination()

    @is_paginated = per_page.present? && page.present?
  end

  def load(record)
    unless record.is_a?(@model)
      raise TypeError.new("#{@model} loader can't load association for #{record.class}")
    end

    super
  end

  def cache_key(record)
    record.object_id
  end

  def perform(records)
    preload_association(records)
    records.each { |record| fulfill(record, read_association(record)) }
  end

  private

  def validate
    if @association.nil?
      raise ArgumentError.new("No association #{@association_name} on #{@model}")
    end
  end

  def validate_pagination
    if (@page.present? && !@page.positive?) || (@per_page.present? && !@per_page.positive?)
      raise TypeError.new("#{@model} loader can't load association with page #{@page} and per_page #{@per_page}")
    end
  end

  def preload_association(records)
    if @use_preloader
      ::ActiveRecord::Associations::Preloader.new.preload(records, @association_name, @scope)
    else
      @bucket = {}

      results = []

      model_primary_key_query = "#{@model_table_name}.#{@model_primary_key}"
      association_columns = @association_model.attribute_names.map { |c| "#{@association_model.table_name}.#{c}" }.join(', ')

      if @is_paginated
        scope_order = if @order.present?
                        @order
                      else
                        "#{@association_table_name}.#{@association_primary_key} ASC NULLS LAST"
                      end

        sub_results_query = @model.joins(@association_name)
                                  .select(<<~SQL)
                                    #{model_primary_key_query} AS model_primary_key,
                                    #{association_columns},
                                    ROW_NUMBER() OVER (
                                      PARTITION BY #{model_primary_key_query}
                                      ORDER BY #{scope_order}
                                    ) AS row_num
                                  SQL
                                  .where(<<~SQL, model_ids: records.pluck(:id))
                                    #{model_primary_key_query} IN (:model_ids)
                                  SQL

        sub_results_query = add_scope(sub_results_query, with_association_name: true, add_order: false)

        sub_results_query = sub_results_query.to_sql

        row_min = (@page - 1) * @per_page
        row_max = row_min + @per_page

        results = @association_model.select('*')
                                    .joins(<<~SQL)
                                      LEFT JOIN (#{sub_results_query}) AS t
                                        ON t.#{@association_primary_key} = #{@association_table_name}.#{@association_primary_key}
                                    SQL
                                    .where(<<~SQL)
                                      t.row_num > #{row_min} AND t.row_num <= #{row_max} AND t.#{@association_primary_key} IS NOT NULL
                                    SQL
                                    .order(<<~'SQL')
                                      t.row_num ASC NULLS LAST
                                    SQL
      else
        results = @model.joins(@association_name)
                        .select(<<~SQL)
                          #{model_primary_key_query} AS model_primary_key,
                          #{association_columns}
                        SQL
                        .where(<<~SQL, model_ids: records.pluck(:id))
                          #{model_primary_key_query} IN (:model_ids)
                        SQL

        results = add_scope(results, with_association_name: true, add_order: true)
      end

      results.each do |result|
        key = result.send('model_primary_key')

        @bucket[key] ||= []
        @bucket[key] << result
      end
    end
  end

  def read_association(record)
    if @use_preloader
      if @is_paginated
        results = record.public_send(@association_name)

        results = add_scope(results)

        results.offset((@page - 1) * @per_page)
               .limit(@per_page)
      else
        record.public_send(@association_name)
      end
    else
      key = record.send(@model_primary_key)
      results = @bucket[key] || []

      @is_collection ? results : results.first
    end
  end

  def add_scope(target, with_association_name: false, add_order: true)
    if @distinct.present?
      target = target.distinct()
    end

    if @where.present?
      target = target.where(@where)
    end

    if @joins.present?
      target = if with_association_name
                 target.joins(@association_name => @joins)
               else
                 target.joins(@joins)
               end
    end

    if add_order && @order.present?
      target = target.order(@order)
    end

    target
  end
end
