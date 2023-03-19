class Loaders::Association < GraphQL::Batch::Loader
  attr_reader :distinct, :where, :joins, :order, :page, :per_page
  attr_reader :model, :association_name, :association_model
  # attr_reader :is_collection, :model_primary_key, :association_primary_key

  attr_accessor :scope

  def initialize(model, association_name, scope: nil, distinct: false, where: {}, joins: {}, order: {}, page: nil, per_page: nil)
    @model = model
    # @model_primary_key = model.primary_key

    @association_name = association_name

    @association = @model.reflect_on_association(association_name)
    @association_model = nil

    # @is_collection = @association.collection?

    unless @association.polymorphic?
      @association_model = @association.klass
      # @association_primary_key = @association_model.primary_key
    end

    validate()

    @page = page
    @per_page = per_page

    validate_pagination()

    @is_paginated = per_page.present? && page.present?

    @distinct = distinct
    @where = where
    @joins = joins
    @order = order

    @scope = scope

    if @scope.nil? && @association_model.present? && (@distinct.present? || @where.present? || @joins.present? || @order.present?)
      @scope = @association_model

      if @distinct.present?
        @scope = @scope.distinct()
      end

      if @where.present?
        @scope = @scope.where(@where)
      end

      if @joins.present?
        @scope = @scope.joins(@joins)
      end

      if @order.present?
        @scope = @scope.order(@order)
      end
    end
  end

  def load(record)
    unless record.is_a?(@model)
      raise TypeError.new("#{@model} loader can't load association for #{record.class}")
    end

    if association_loaded?(record)
      return Promise.resolve(read_association(record))
    end

    super
  end

  def cache_key(record)
    record.object_id
  end

  def perform(records)
    unless @is_paginated
      preload_association(records)
    end

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

  def association_loaded?(record)
    record.association(@association_name).loaded?
  end

  def preload_association(records)
    ::ActiveRecord::Associations::Preloader.new.preload(records, @association_name, @scope)
  end

  def read_association(record)
    if @is_paginated
      results = record.public_send(@association_name)

      if @distinct.present?
        results = results.distinct()
      end

      if @where.present?
        results = results.where(@where)
      end

      if @joins.present?
        results = results.joins(@joins)
      end

      if @order.present?
        results = results.order(@order)
      end

      results.offset((@page - 1) * @per_page)
             .limit(@per_page)
    else
      record.public_send(@association_name)
    end
  end
end
