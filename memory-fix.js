/*
 * V24 — processamento incremental com memória limitada.
 * Substitui os handlers legados sem alterar as análises existentes.
 */
(() => {
  'use strict';

  const STREAM_FIELDS = [
    'NR_ANO_MES', 'DT', 'DT_VENDA', 'COD_MUNICIPIO',
    'NM_INDICADOR_NEGOCIO', 'NM_MARCA', 'NM_LINHA_NEGOCIO',
    'NM_PERFIL_CLIENTE', 'NM_TIPO_ASS_DOMICILIO', 'CD_NODE'
  ];
  const REQUIRED = {
    ND: ['COD_MUNICIPIO', 'NM_INDICADOR_NEGOCIO', 'QT'],
    BL: ['COD_MUNICIPIO', 'NM_INDICADOR_NEGOCIO', 'NM_MARCA', 'DT', 'DT_VENDA', 'QT']
  };
  const streamState = {
    active: null,
    finalizing: false,
    batchLoading: false,
    datasets: {ND: null, BL: null},
    directoryHandle: null,
    directoryName: '',
    manifestLastModified: 0,
    refreshTimer: null,
    manualManifest: null
  };

  const WORKER_SOURCE = String.raw`
    'use strict';
    const FIELDS = ${JSON.stringify(['NR_ANO_MES','DT','DT_VENDA','COD_MUNICIPIO','NM_INDICADOR_NEGOCIO','NM_MARCA','NM_LINHA_NEGOCIO','NM_PERFIL_CLIENTE','NM_TIPO_ASS_DOMICILIO','CD_NODE'])};
    const REQUIRED = ${JSON.stringify({ND:['COD_MUNICIPIO','NM_INDICADOR_NEGOCIO','QT'],BL:['COD_MUNICIPIO','NM_INDICADOR_NEGOCIO','NM_MARCA','DT','DT_VENDA','QT']})};
    const CHUNK_SIZE = 2 * 1024 * 1024;
    const clean = value => String(value == null ? '' : value).normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
    const norm = value => clean(value).toUpperCase();
    const numberValue = value => Number(String(value || 0).replace(',', '.')) || 0;

    async function encodingOf(file) {
      let sample = await file.slice(0, Math.min(file.size, 1024 * 1024)).arrayBuffer();
      try {
        new TextDecoder('utf-8', {fatal: true}).decode(sample);
        sample = null;
        return 'utf-8';
      } catch (_) {
        sample = null;
        return 'windows-1252';
      }
    }

    self.onmessage = async event => {
      const file = event.data.file;
      const type = event.data.type;
      let aggregate = new Map();
      const dictionaries = FIELDS.map(() => []);
      const dictionaryIndexes = FIELDS.map(() => new Map());
      let headers = null;
      let positions = null;
      let qtPosition = -1;
      let sourceRows = 0;
      let hasNet = false;
      let competence = '';
      let maxDate = '';
      let field = '';
      let row = [];
      let inQuotes = false;
      let pendingQuote = false;
      let decoder = null;

      const dictionaryCode = (index, value) => {
        const map = dictionaryIndexes[index];
        let code = map.get(value);
        if (code === undefined) {
          code = dictionaries[index].length;
          dictionaries[index].push(value);
          map.set(value, code);
        }
        return code;
      };

      const consumeRow = values => {
        if (!values.some(Boolean)) return;
        if (!headers) {
          headers = values.map((value, index) => clean(value).replace(index === 0 ? /^\uFEFF/ : /$^/, ''));
          const missing = REQUIRED[type].filter(name => !headers.includes(name));
          if (missing.length) throw new Error('Colunas obrigatorias ausentes: ' + missing.join(', '));
          positions = FIELDS.map(name => headers.indexOf(name));
          qtPosition = headers.indexOf('QT');
          return;
        }

        sourceRows++;
        const codes = new Array(FIELDS.length);
        for (let i = 0; i < FIELDS.length; i++) {
          const value = positions[i] < 0 ? '' : clean(values[positions[i]] || '');
          codes[i] = dictionaryCode(i, value);
          if (!competence && FIELDS[i] === 'NR_ANO_MES' && value) competence = value;
          if (FIELDS[i] === 'DT' && value > maxDate) maxDate = value;
          if (FIELDS[i] === 'NM_MARCA' && norm(value) === 'NET') hasNet = true;
        }
        const key = codes.join(',');
        let item = aggregate.get(key);
        if (!item) {
          item = {codes, qt: 0};
          aggregate.set(key, item);
        }
        item.qt += numberValue(values[qtPosition]);
      };

      const parseText = (text, final) => {
        let i = 0;
        while (i < text.length) {
          let ch = text[i];
          if (pendingQuote) {
            pendingQuote = false;
            if (ch === '"') {
              field += '"';
              i++;
              continue;
            }
            inQuotes = false;
          }
          if (ch === '"') {
            if (inQuotes) {
              if (i + 1 < text.length) {
                if (text[i + 1] === '"') {
                  field += '"';
                  i += 2;
                  continue;
                }
                inQuotes = false;
              } else {
                pendingQuote = true;
              }
            } else {
              inQuotes = true;
            }
          } else if (ch === ';' && !inQuotes) {
            row.push(field);
            field = '';
          } else if ((ch === '\n' || ch === '\r') && !inQuotes) {
            if (ch === '\r' && text[i + 1] === '\n') i++;
            row.push(field);
            field = '';
            consumeRow(row);
            row = [];
          } else {
            field += ch;
          }
          i++;
        }
        if (final) {
          if (pendingQuote) {
            pendingQuote = false;
            inQuotes = false;
          }
          if (field || row.length) {
            row.push(field);
            consumeRow(row);
            field = '';
            row = [];
          }
        }
      };

      try {
        const encoding = await encodingOf(file);
        decoder = new TextDecoder(encoding);
        let offset = 0;
        let lastPercent = -1;
        while (offset < file.size) {
          const end = Math.min(file.size, offset + CHUNK_SIZE);
          let buffer = await file.slice(offset, end).arrayBuffer();
          let text = decoder.decode(buffer, {stream: end < file.size});
          buffer = null;
          parseText(text, false);
          text = null;
          offset = end;
          const percent = Math.floor(offset / Math.max(1, file.size) * 100);
          if (percent >= lastPercent + 2 || offset === file.size) {
            lastPercent = percent;
            self.postMessage({kind: 'progress', percent, sourceRows, compactRows: aggregate.size});
          }
        }
        const tail = decoder.decode();
        if (tail) parseText(tail, false);
        parseText('', true);
        decoder = null;

        if (!headers) throw new Error('Arquivo vazio ou sem cabecalho.');
        if (type === 'BL' && !hasNet) throw new Error('Arquivo BL invalido: sem registros NM_MARCA = NET.');

        const rows = [];
        for (const item of aggregate.values()) {
          const output = {QT: item.qt, ORIGEM_CONVERSAO: type};
          for (let i = 0; i < FIELDS.length; i++) {
            const value = dictionaries[i][item.codes[i]];
            if (value !== '') output[FIELDS[i]] = value;
          }
          rows.push(output);
        }
        const result = {
          kind: 'done', type, rows, headers, sourceRows,
          compactRows: rows.length, competence, maxDate, encoding
        };
        aggregate.clear();
        aggregate = null;
        dictionaryIndexes.forEach(map => map.clear());
        self.postMessage(result);
      } catch (error) {
        if (aggregate) aggregate.clear();
        dictionaryIndexes.forEach(map => map.clear());
        self.postMessage({kind: 'error', type, message: error && error.message ? error.message : String(error)});
      }
    };
  `;

  const JSON_WORKER_SOURCE = String.raw`
    'use strict';
    const FIELDS = ${JSON.stringify(['NR_ANO_MES','DT','DT_VENDA','COD_MUNICIPIO','NM_INDICADOR_NEGOCIO','NM_MARCA','NM_LINHA_NEGOCIO','NM_PERFIL_CLIENTE','NM_TIPO_ASS_DOMICILIO','CD_NODE'])};
    const clean = value => String(value == null ? '' : value).normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
    const norm = value => clean(value).toUpperCase();
    const numberValue = value => Number(String(value == null ? 0 : value).replace(',', '.')) || 0;

    async function fileText(file) {
      if (/\.gz$/i.test(file.name)) {
        if (typeof DecompressionStream === 'undefined') {
          throw new Error('Este navegador não oferece DecompressionStream para abrir .json.gz. Use Edge/Chrome atualizado ou selecione o .json sem compactação.');
        }
        return new Response(file.stream().pipeThrough(new DecompressionStream('gzip'))).text();
      }
      return file.text();
    }

    function stableKey(values) {
      return values.map(value => value.length + ':' + value).join('|');
    }

    self.onmessage = async event => {
      const files = Array.from(event.data.files || []);
      const expectedType = event.data.type;
      let aggregate = new Map();
      let sourceRows = 0;
      let competence = '';
      let maxDate = '';
      let hasNet = false;
      let fileCount = 0;

      try {
        if (!files.length) throw new Error('Nenhum arquivo JSON selecionado.');
        for (let fileIndex = 0; fileIndex < files.length; fileIndex++) {
          const file = files[fileIndex];
          self.postMessage({kind: 'progress', stage: 'reading', fileIndex, fileCount: files.length, fileName: file.name, compactRows: aggregate.size});
          let text = await fileText(file);
          let payload;
          try {
            payload = JSON.parse(text);
          } catch (error) {
            throw new Error(file.name + ': JSON inválido (' + (error && error.message ? error.message : error) + ').');
          } finally {
            text = null;
          }

          if (!payload || payload.v !== 4) throw new Error(file.name + ': esperado JSON colunar v4.');
          const payloadType = norm(payload.t || expectedType);
          if (payloadType !== expectedType) throw new Error(file.name + ': tipo ' + (payload.t || 'não informado') + ' incompatível com ' + expectedType + '.');
          if (!Array.isArray(payload.c) || !Array.isArray(payload.d) || !Array.isArray(payload.r)) {
            throw new Error(file.name + ': estrutura v4 incompleta; esperados c, d e r.');
          }
          if (payload.d.length !== payload.c.length) throw new Error(file.name + ': quantidade de dicionários diferente da lista de campos.');

          const positions = FIELDS.map(field => payload.c.indexOf(field));
          const required = expectedType === 'BL'
            ? ['COD_MUNICIPIO', 'NM_INDICADOR_NEGOCIO', 'NM_MARCA', 'DT', 'DT_VENDA']
            : ['COD_MUNICIPIO', 'NM_INDICADOR_NEGOCIO'];
          const missing = required.filter(field => !payload.c.includes(field));
          if (missing.length) throw new Error(file.name + ': campos obrigatórios ausentes: ' + missing.join(', ') + '.');

          for (let rowIndex = 0; rowIndex < payload.r.length; rowIndex++) {
            const packed = payload.r[rowIndex];
            if (!Array.isArray(packed) || packed.length < payload.c.length + 1) {
              throw new Error(file.name + ': linha compacta inválida na posição ' + (rowIndex + 1) + '.');
            }
            const values = new Array(FIELDS.length);
            for (let fieldIndex = 0; fieldIndex < FIELDS.length; fieldIndex++) {
              const sourcePosition = positions[fieldIndex];
              if (sourcePosition < 0) {
                values[fieldIndex] = '';
                continue;
              }
              const dictionary = payload.d[sourcePosition];
              const dictionaryCode = packed[sourcePosition];
              if (!Array.isArray(dictionary) || !Number.isInteger(dictionaryCode) || dictionaryCode < 0 || dictionaryCode >= dictionary.length) {
                throw new Error(file.name + ': código de dicionário inválido em ' + FIELDS[fieldIndex] + ', linha ' + (rowIndex + 1) + '.');
              }
              values[fieldIndex] = clean(dictionary[dictionaryCode]);
            }
            if (!competence && values[0]) competence = values[0];
            if (values[1] > maxDate) maxDate = values[1];
            if (norm(values[5]) === 'NET') hasNet = true;
            const key = stableKey(values);
            let item = aggregate.get(key);
            if (!item) {
              item = {values, qt: 0};
              aggregate.set(key, item);
            }
            item.qt += numberValue(packed[payload.c.length]);
            if (rowIndex && rowIndex % 25000 === 0) {
              self.postMessage({kind: 'progress', stage: 'decoding', fileIndex, fileCount: files.length, fileName: file.name, rowIndex, rowCount: payload.r.length, compactRows: aggregate.size});
            }
          }
          sourceRows += Number(payload.sourceRows) || payload.r.length;
          fileCount++;
          payload = null;
        }

        if (expectedType === 'BL' && !hasNet) throw new Error('Conjunto JSON BL inválido: sem registros NM_MARCA = NET.');
        const rows = [];
        for (const item of aggregate.values()) {
          const output = {QT: item.qt, ORIGEM_CONVERSAO: expectedType};
          for (let i = 0; i < FIELDS.length; i++) if (item.values[i] !== '') output[FIELDS[i]] = item.values[i];
          rows.push(output);
        }
        aggregate.clear();
        aggregate = null;
        self.postMessage({
          kind: 'done', type: expectedType, rows, sourceRows, compactRows: rows.length,
          competence, maxDate, encoding: 'JSON colunar v4', fileCount
        });
      } catch (error) {
        if (aggregate) aggregate.clear();
        self.postMessage({kind: 'error', type: expectedType, message: error && error.message ? error.message : String(error)});
      }
    };
  `;

  function installDataSourcePanel() {
    if ($('#oneDriveDataSource')) return;
    const panel = document.createElement('div');
    panel.id = 'oneDriveDataSource';
    panel.className = 'oneDriveDataSource';
    panel.innerHTML = `
      <div class="sourcePanelHeader">
        <div>
          <h2>Carregar JSON sem publicar as bases</h2>
          <p>Leia <b>manifest.json</b> e JSONs ND/BL diretamente de uma pasta local sincronizada pelo OneDrive.</p>
        </div>
        <div class="folderButtons">
          <button id="selectOneDriveFolder" type="button" class="folderPickerBtn">Selecionar pasta do OneDrive</button>
          <button id="refreshOneDriveFolder" type="button" disabled>Atualizar pasta selecionada</button>
        </div>
        <input id="oneDriveDirectoryInput" type="file" webkitdirectory multiple hidden>
      </div>
      <p class="folderPathHint">Pasta esperada neste computador: <code>C:\\Users\\f273184\\OneDrive - Claro SA\\relatorios- Instalação - Otavio\\RELATORIOS DIARIOS\\dashbord\\dashboard github - base arquivo</code>. Por segurança, o navegador exige que a pasta seja escolhida pelo usuário na primeira vez.</p>
      <div class="jsonSourceGrid">
        <label>JSON ND individual ou partes
          <input type="file" id="jsonND" accept=".json,.gz,application/json,application/gzip" multiple>
        </label>
        <label>JSON BL individual ou partes
          <input type="file" id="jsonBL" accept=".json,.gz,application/json,application/gzip" multiple>
        </label>
      </div>
      <div class="sourceActions">
        <button id="clearND" type="button" class="clearBaseBtn" disabled>Limpar base ND</button>
        <button id="clearBL" type="button" class="clearBaseBtn" disabled>Limpar base BL</button>
        <button id="downloadManifest" type="button" disabled>Baixar manifest.json</button>
        <label class="autoRefreshLabel"><input id="autoRefreshOneDrive" type="checkbox" checked> Atualizar automaticamente a cada 60 s</label>
      </div>
      <div id="folderStatus" class="status" aria-live="polite">Aguardando seleção da pasta do OneDrive, JSON individual ou CSV manual.</div>`;
    const actions = document.querySelector('#upload .baseActions');
    actions.insertAdjacentElement('afterend', panel);

    const style = document.createElement('style');
    style.textContent = `
      .oneDriveDataSource{margin-top:14px;padding:16px;border:1px solid #d8dde5;border-left:5px solid #da291c;border-radius:10px;background:#fff}
      .sourcePanelHeader{display:flex;align-items:center;justify-content:space-between;gap:18px}
      .sourcePanelHeader h2{margin:0 0 4px;font-size:1.05rem;color:#20242b}.sourcePanelHeader p{margin:0;color:#4c5563}
      .folderPickerBtn{background:#20242b;color:#fff;border-color:#20242b;white-space:nowrap}.folderPickerBtn:hover{background:#050607}
      .folderButtons{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
      .folderPathHint{margin:12px 0;color:#3d4653;font-size:.84rem;line-height:1.45}.folderPathHint code{overflow-wrap:anywhere;color:#8f1710;font-weight:700}
      .jsonSourceGrid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:10px}.jsonSourceGrid label{font-weight:800;color:#303743}
      .jsonSourceGrid input{display:block;width:100%;margin-top:6px;font-weight:500}
      .sourceActions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:12px}.clearBaseBtn{background:#fff;color:#9f1d14;border-color:#d6a19d}.clearBaseBtn:not(:disabled):hover{background:#fff2f1}.autoRefreshLabel{display:flex;align-items:center;gap:7px;margin-left:auto;font-size:.84rem;color:#3d4653}.autoRefreshLabel input{width:auto}
      #folderStatus{margin-top:10px}.oneDriveDataSource button:disabled{opacity:.5;cursor:not-allowed}
      @media(max-width:760px){.sourcePanelHeader{align-items:stretch;flex-direction:column}.jsonSourceGrid{grid-template-columns:1fr}.folderButtons>*{width:100%}.autoRefreshLabel{width:100%;margin-left:0}}
    `;
    document.head.appendChild(style);
  }

  function replaceFileInput(id) {
    const oldInput = $('#' + id);
    const input = oldInput.cloneNode(true);
    oldInput.replaceWith(input);
    return input;
  }

  function setImportLock(locked) {
    const effectiveLock = !!locked || streamState.batchLoading;
    $('#file').disabled = effectiveLock;
    $('#fileBL').disabled = effectiveLock;
    ['#selectOneDriveFolder', '#oneDriveDirectoryInput', '#jsonND', '#jsonBL'].forEach(id => {
      const element = $(id);
      if (element) element.disabled = effectiveLock;
    });
    $('#convertJsonND').disabled = effectiveLock || !streamState.datasets.ND;
    $('#convertJsonBL').disabled = effectiveLock || !streamState.datasets.BL;
    $('#downloadJsonND').disabled = effectiveLock || !jsonDownloads.ND;
    $('#downloadJsonBL').disabled = effectiveLock || !jsonDownloads.BL;
    if ($('#clearND')) $('#clearND').disabled = effectiveLock || !streamState.datasets.ND;
    if ($('#clearBL')) $('#clearBL').disabled = effectiveLock || !streamState.datasets.BL;
    if ($('#refreshOneDriveFolder')) $('#refreshOneDriveFolder').disabled = effectiveLock || !streamState.directoryHandle;
    if ($('#downloadManifest')) $('#downloadManifest').disabled = effectiveLock || !streamState.manualManifest;
  }

  function rowsForType(type) {
    const direct = type === 'ND' ? P : PB;
    if (direct && direct.length) return direct;
    if (!J.length) return [];
    const rows = [];
    for (const row of J) if (row.ORIGEM_CONVERSAO === type) rows.push(row);
    return rows;
  }

  function forEachTypeRow(type, callback) {
    const direct = type === 'ND' ? P : PB;
    if (direct && direct.length) {
      for (let i = 0; i < direct.length; i++) callback(direct[i], i, direct.length);
      return direct.length;
    }
    let count = 0;
    for (const row of J) if (row.ORIGEM_CONVERSAO === type) count++;
    let index = 0;
    for (const row of J) if (row.ORIGEM_CONVERSAO === type) callback(row, index++, count);
    return count;
  }

  function prepareReload(type) {
    if (J.length) {
      const opposite = [];
      for (const row of J) if (row.ORIGEM_CONVERSAO !== type) opposite.push(row);
      if (type === 'ND') {
        P = [];
        PB = opposite;
      } else {
        PB = [];
        P = opposite;
      }
      J = [];
    } else if (type === 'ND') {
      P = [];
    } else {
      PB = [];
    }
    streamState.datasets[type] = null;
    jsonDownloads[type] = null;
    if (type === 'ND') okND = false;
    else okBL = false;
    ['#tAna', '#tDia', '#tMes', '#tConv'].forEach(id => $(id).disabled = true);
    $('#validar').disabled = true;
  }

  function createParserWorker() {
    const url = URL.createObjectURL(new Blob([WORKER_SOURCE], {type: 'text/javascript'}));
    const worker = new Worker(url);
    return {worker, url};
  }

  function createJsonParserWorker() {
    const url = URL.createObjectURL(new Blob([JSON_WORKER_SOURCE], {type: 'text/javascript'}));
    const worker = new Worker(url);
    return {worker, url};
  }

  function storeDataset(type, message, source) {
    if (type === 'ND') {
      P = message.rows;
      okND = true;
    } else {
      PB = message.rows;
      okBL = true;
    }
    streamState.datasets[type] = {
      sourceRows: message.sourceRows,
      compactRows: message.compactRows,
      competence: message.competence,
      maxDate: message.maxDate,
      encoding: message.encoding,
      source: source || 'CSV',
      fileCount: message.fileCount || 1
    };
    $('#' + (type === 'ND' ? 'analisar' : 'analisarBL')).disabled = false;
    $('#' + (type === 'ND' ? 'convertJsonND' : 'convertJsonBL')).disabled = false;
  }

  async function processCsv(type, file, input) {
    if (!file || streamState.active) return;
    prepareReload(type);
    streamState.active = type;
    setImportLock(true);
    const statusId = type === 'ND' ? '#stND' : '#stBL';
    const loadingId = type === 'ND' ? '#loadND' : '#loadBL';
    setLoading(loadingId, true);
    setStatus(statusId, `Lendo ${type} em blocos de 2 MB no Web Worker... 0%`, 'processing');
    await nextPaint();

    const {worker, url} = createParserWorker();
    let heartbeat = 0;
    const heartbeatTimer = setInterval(() => {
      heartbeat++;
      document.documentElement.dataset.workerHeartbeat = String(heartbeat);
    }, 100);

    const cleanup = () => {
      clearInterval(heartbeatTimer);
      worker.terminate();
      URL.revokeObjectURL(url);
      streamState.active = null;
      input.value = '';
      setLoading(loadingId, false);
      setImportLock(false);
    };

    worker.onmessage = async event => {
      const message = event.data;
      if (message.kind === 'progress') {
        setStatus(
          statusId,
          `${type}: ${message.percent}% — ${message.sourceRows.toLocaleString('pt-BR')} linhas lidas; ` +
          `${message.compactRows.toLocaleString('pt-BR')} combinações compactas. Interface responsiva.`,
          'processing'
        );
        return;
      }
      if (message.kind === 'error') {
        cleanup();
        setStatus(statusId, `Falha ao processar ${type}: ${message.message}`, 'error');
        return;
      }
      if (message.kind !== 'done') return;

      storeDataset(type, message, 'CSV manual');
      cleanup();
      setStatus(
        statusId,
        `${type} aprovado: ${message.sourceRows.toLocaleString('pt-BR')} linhas processadas em blocos e ` +
        `${message.compactRows.toLocaleString('pt-BR')} combinações mantidas em memória. ` +
        `Texto bruto e blocos temporários liberados.`,
        'success'
      );
      $('#checks').innerHTML = `Processamento incremental ativo. ${type}: ` +
        `${message.sourceRows.toLocaleString('pt-BR')} linhas de origem → ` +
        `${message.compactRows.toLocaleString('pt-BR')} combinações compactas.`;
      await nextPaint();
      if (okND && okBL) await finalizeAnalyses();
    };

    worker.onerror = event => {
      cleanup();
      setStatus(statusId, `Falha no Web Worker ${type}: ${event.message || 'erro inesperado'}`, 'error');
    };

    worker.postMessage({file, type});
  }

  async function processJsonFiles(type, selectedFiles, sourceLabel) {
    const files = Array.from(selectedFiles || []).filter(file => /\.json(?:\.gz)?$/i.test(file.name));
    if (!files.length || streamState.active) return false;
    prepareReload(type);
    streamState.active = type;
    setImportLock(true);
    const statusId = type === 'ND' ? '#stND' : '#stBL';
    const loadingId = type === 'ND' ? '#loadND' : '#loadBL';
    setLoading(loadingId, true);
    setStatus(statusId, `Lendo ${files.length.toLocaleString('pt-BR')} arquivo(s) JSON ${type} no Web Worker...`, 'processing');
    await nextPaint();

    const {worker, url} = createJsonParserWorker();
    const cleanup = () => {
      worker.terminate();
      URL.revokeObjectURL(url);
      streamState.active = null;
      setLoading(loadingId, false);
      setImportLock(false);
    };

    return new Promise(resolve => {
      worker.onmessage = async event => {
        const message = event.data;
        if (message.kind === 'progress') {
          const position = Math.min(message.fileCount, message.fileIndex + 1);
          const detail = message.stage === 'decoding'
            ? ` — ${message.rowIndex.toLocaleString('pt-BR')} de ${message.rowCount.toLocaleString('pt-BR')} linhas compactas`
            : '';
          setStatus(
            statusId,
            `${type}: arquivo ${position} de ${message.fileCount} (${message.fileName})${detail}; ` +
            `${message.compactRows.toLocaleString('pt-BR')} combinações acumuladas. Interface responsiva.`,
            'processing'
          );
          return;
        }
        if (message.kind === 'error') {
          cleanup();
          setStatus(statusId, `Falha ao carregar JSON ${type}: ${message.message}`, 'error');
          resolve(false);
          return;
        }
        if (message.kind !== 'done') return;

        storeDataset(type, message, sourceLabel || 'JSON individual');
        cleanup();
        setStatus(
          statusId,
          `${type} aprovado via ${sourceLabel || 'JSON'}: ${message.fileCount.toLocaleString('pt-BR')} arquivo(s), ` +
          `${message.sourceRows.toLocaleString('pt-BR')} linhas de origem e ` +
          `${message.compactRows.toLocaleString('pt-BR')} combinações v4 carregadas.`,
          'success'
        );
        $('#checks').innerHTML = `JSON colunar v4 ${type} validado no Web Worker: ` +
          `${message.sourceRows.toLocaleString('pt-BR')} linhas de origem → ` +
          `${message.compactRows.toLocaleString('pt-BR')} combinações compactas.`;
        await nextPaint();
        if (okND && okBL && !streamState.batchLoading) await finalizeAnalyses();
        resolve(true);
      };

      worker.onerror = event => {
        cleanup();
        setStatus(statusId, `Falha no Web Worker JSON ${type}: ${event.message || 'erro inesperado'}`, 'error');
        resolve(false);
      };
      worker.postMessage({files, type});
    });
  }

  function normalizeRelativePath(path) {
    const cleanPath = String(path || '').replace(/\\/g, '/').replace(/^\.\//, '').replace(/^\/+/, '');
    const parts = cleanPath.split('/').filter(Boolean);
    if (parts.includes('..') || /^[a-z]:/i.test(cleanPath) || /^https?:/i.test(cleanPath)) {
      throw new Error('O manifest.json deve usar apenas caminhos relativos dentro da pasta selecionada.');
    }
    return parts.join('/');
  }

  function manifestPaths(manifest, type) {
    const result = [];
    const seen = new Set();
    const add = value => {
      if (typeof value !== 'string' || !value.trim()) return;
      const path = normalizeRelativePath(value.trim());
      if (!/\.json(?:\.gz)?$/i.test(path) || /(^|\/)manifest\.json$/i.test(path) || seen.has(path)) return;
      seen.add(path);
      result.push(path);
    };
    const visit = value => {
      if (typeof value === 'string') {
        add(value);
      } else if (Array.isArray(value)) {
        value.forEach(visit);
      } else if (value && typeof value === 'object') {
        ['file', 'path', 'name'].forEach(key => add(value[key]));
        ['files', 'parts'].forEach(key => visit(value[key]));
      }
    };
    const datasets = manifest && manifest.datasets ? manifest.datasets : {};
    visit(datasets[type.toLowerCase()]);
    return result;
  }

  function relativeFiles(fileList) {
    const records = Array.from(fileList || []).map(file => ({
      file,
      path: normalizeRelativePath(file.webkitRelativePath || file.name)
    }));
    if (!records.length) return records;
    const firstSegments = new Set(records.map(record => record.path.split('/')[0]));
    if (firstSegments.size === 1 && records.some(record => record.path.includes('/'))) {
      for (const record of records) record.path = record.path.split('/').slice(1).join('/');
    }
    return records;
  }

  async function collectDirectoryFiles(directoryHandle, prefix, records) {
    for await (const [name, entry] of directoryHandle.entries()) {
      const path = prefix ? `${prefix}/${name}` : name;
      if (entry.kind === 'directory') {
        await collectDirectoryFiles(entry, path, records);
      } else if (/manifest\.json$|\.json(?:\.gz)?$/i.test(name)) {
        records.push({path: normalizeRelativePath(path), file: await entry.getFile()});
      }
    }
  }

  function buildFolderIndex(records, basePath) {
    const index = new Map();
    const basenameCount = new Map();
    const normalizedBase = basePath ? normalizeRelativePath(basePath).replace(/\/$/, '') : '';
    for (const record of records) {
      const full = normalizeRelativePath(record.path);
      let relative = full;
      if (normalizedBase && full.startsWith(normalizedBase + '/')) relative = full.slice(normalizedBase.length + 1);
      index.set(relative.toLowerCase(), record.file);
      const base = relative.split('/').pop().toLowerCase();
      basenameCount.set(base, (basenameCount.get(base) || 0) + 1);
    }
    for (const record of records) {
      const full = normalizeRelativePath(record.path);
      let relative = full;
      if (normalizedBase && full.startsWith(normalizedBase + '/')) relative = full.slice(normalizedBase.length + 1);
      const base = relative.split('/').pop().toLowerCase();
      if (basenameCount.get(base) === 1) index.set(base, record.file);
    }
    return index;
  }

  async function loadFolderRecords(records, folderName) {
    if (!records.length) throw new Error('A pasta selecionada não contém manifest.json nem arquivos JSON.');
    const manifests = records.filter(record => /(^|\/)manifest\.json$/i.test(record.path))
      .sort((a, b) => a.path.split('/').length - b.path.split('/').length || a.path.localeCompare(b.path));
    if (!manifests.length) throw new Error('manifest.json não encontrado na pasta selecionada.');
    const manifestRecord = manifests[0];
    let manifest;
    try {
      manifest = JSON.parse(await manifestRecord.file.text());
    } catch (error) {
      throw new Error('manifest.json inválido: ' + (error && error.message ? error.message : error));
    }
    const basePath = manifestRecord.path.includes('/') ? manifestRecord.path.split('/').slice(0, -1).join('/') : '';
    const index = buildFolderIndex(records, basePath);
    const allDataFiles = records.filter(record => !/(^|\/)manifest\.json$/i.test(record.path));
    const plan = {};
    for (const type of ['ND', 'BL']) {
      let paths = manifestPaths(manifest, type);
      if (!paths.length) {
        paths = allDataFiles
          .map(record => {
            const full = normalizeRelativePath(record.path);
            return basePath && full.startsWith(basePath + '/') ? full.slice(basePath.length + 1) : full;
          })
          .filter(path => new RegExp('^' + type.toLowerCase() + '(?:[-_].*)?\\.json(?:\\.gz)?$', 'i').test(path.split('/').pop()))
          .sort((a, b) => a.localeCompare(b, 'pt-BR'));
      }
      plan[type] = paths.map(path => {
        const file = index.get(normalizeRelativePath(path).toLowerCase());
        if (!file) throw new Error(`Arquivo ${path} indicado para ${type} não foi encontrado na pasta selecionada.`);
        return file;
      });
    }

    if (!plan.ND.length && !plan.BL.length) {
      setStatus('#folderStatus', `Pasta ${folderName || ''} vinculada. O manifest.json ainda não contém JSONs ND/BL; carregue CSVs e salve os JSONs diretamente aqui.`, 'success');
      return false;
    }
    streamState.batchLoading = true;
    setImportLock(true);
    setStatus('#folderStatus', `Manifesto carregado de ${folderName || 'pasta selecionada'}. Processando ND e BL em sequência...`, 'processing');
    let ndOk = !plan.ND.length;
    let blOk = !plan.BL.length;
    try {
      if (plan.ND.length) ndOk = await processJsonFiles('ND', plan.ND, 'pasta OneDrive');
      if (ndOk && plan.BL.length) blOk = await processJsonFiles('BL', plan.BL, 'pasta OneDrive');
    } finally {
      streamState.batchLoading = false;
      setImportLock(false);
    }
    if (!plan.ND.length) setStatus('#stND', 'Nenhum JSON ND foi definido ou detectado no manifest.json.', 'error');
    if (!plan.BL.length) setStatus('#stBL', 'Nenhum JSON BL foi definido ou detectado no manifest.json.', 'error');
    if (ndOk && blOk && okND && okBL) {
      setStatus('#folderStatus', `Pasta ${folderName || ''} validada: manifest.json + JSONs ND/BL carregados.`, 'success');
      await finalizeAnalyses();
      return true;
    }
    setStatus('#folderStatus', 'Carga da pasta concluída parcialmente. Verifique os avisos das bases ND e BL.', 'error');
    return false;
  }

  async function chooseOneDriveFolder() {
    if (streamState.active || streamState.batchLoading) return;
    if ('showDirectoryPicker' in window && window.isSecureContext) {
      try {
        const handle = await window.showDirectoryPicker({id: 'dashboard-conversao-v24-dados', mode: 'readwrite'});
        streamState.directoryHandle = handle;
        streamState.directoryName = handle.name;
        const records = [];
        setStatus('#folderStatus', `Lendo manifest.json da pasta ${handle.name}...`, 'processing');
        await collectDirectoryFiles(handle, '', records);
        const manifestRecord = records.find(record => /(^|\/)manifest\.json$/i.test(record.path));
        streamState.manifestLastModified = manifestRecord ? manifestRecord.file.lastModified : 0;
        setImportLock(false);
        if (records.some(record => /(^|\/)manifest\.json$/i.test(record.path))) {
          await loadFolderRecords(records, handle.name);
        } else {
          setStatus('#folderStatus', `Pasta ${handle.name} vinculada. Ao salvar o primeiro JSON, o manifest.json será criado automaticamente.`, 'success');
        }
        startDirectoryRefresh();
      } catch (error) {
        if (error && error.name === 'AbortError') return;
        setStatus('#folderStatus', 'Falha ao abrir a pasta: ' + (error && error.message ? error.message : error), 'error');
      }
      return;
    }
    $('#oneDriveDirectoryInput').click();
  }

  async function refreshSelectedDirectory(silent) {
    const handle = streamState.directoryHandle;
    if (!handle || streamState.active || streamState.batchLoading || streamState.finalizing) return false;
    try {
      const records = [];
      await collectDirectoryFiles(handle, '', records);
      const manifestRecord = records.find(record => /(^|\/)manifest\.json$/i.test(record.path));
      if (silent && manifestRecord && manifestRecord.file.lastModified <= streamState.manifestLastModified) return false;
      if (manifestRecord) streamState.manifestLastModified = manifestRecord.file.lastModified;
      await loadFolderRecords(records, handle.name);
      return true;
    } catch (error) {
      if (!silent) setStatus('#folderStatus', 'Falha ao atualizar a pasta: ' + (error && error.message ? error.message : error), 'error');
      return false;
    }
  }

  function startDirectoryRefresh() {
    if (streamState.refreshTimer) clearInterval(streamState.refreshTimer);
    streamState.refreshTimer = setInterval(() => {
      if ($('#autoRefreshOneDrive') && $('#autoRefreshOneDrive').checked) refreshSelectedDirectory(true);
    }, 60000);
  }

  async function loadDirectoryInput(input) {
    try {
      const records = relativeFiles(input.files);
      const folderName = records[0] && input.files[0].webkitRelativePath
        ? input.files[0].webkitRelativePath.split('/')[0]
        : 'pasta selecionada';
      await loadFolderRecords(records, folderName);
    } catch (error) {
      setStatus('#folderStatus', 'Falha ao carregar a pasta: ' + (error && error.message ? error.message : error), 'error');
    } finally {
      input.value = '';
    }
  }

  function clearDataset(type) {
    if (streamState.active || streamState.finalizing || streamState.batchLoading) return;
    prepareReload(type);
    const input = $(type === 'ND' ? '#jsonND' : '#jsonBL');
    if (input) input.value = '';
    const csvInput = $(type === 'ND' ? '#file' : '#fileBL');
    if (csvInput) csvInput.value = '';
    $('#' + (type === 'ND' ? 'analisar' : 'analisarBL')).disabled = true;
    setStatus(type === 'ND' ? '#stND' : '#stBL', `Base ${type} removida da memória.`, 'success');
    setStatus('#folderStatus', `Base ${type} limpa. A outra base foi preservada, quando existente.`, 'success');
    if (!streamState.datasets.ND && !streamState.datasets.BL) {
      $('#dataArquivo').textContent = '--/--/----';
      $('#baseAtualizada').textContent = '--/--/---- --:--';
    }
    setImportLock(false);
  }

  async function crossIncremental() {
    const groups = new Map();
    for (const criterion of CRIT) {
      const list = groups.get(criterion.COD_MUNICIPIO) || [];
      list.push(criterion);
      groups.set(criterion.COD_MUNICIPIO, list);
    }
    const selected = new Map();
    for (const [code, list] of groups) selected.set(code, list[list.length - 1]);

    J = P.concat(PB);
    P.length = 0;
    PB.length = 0;
    P = [];
    PB = [];

    for (let i = 0; i < J.length; i++) {
      const row = J[i];
      const code = String(row.COD_MUNICIPIO || '').trim();
      const criterion = selected.get(code);
      const conflicting = (groups.get(code) || []).length > 1;
      row.STATUS_CRUZAMENTO = criterion ? (conflicting ? 'CRITERIO CONFLITANTE' : 'CRUZADO') : 'NAO LOCALIZADO';
      row.CRITERIO_CIDADE = criterion ? criterion.CIDADES || '' : '';
      row.CRITERIO_UF = criterion ? criterion.UF || '' : '';
      row.CRITERIO_SUBCLUSTER = criterion ? criterion.SUBCLUSTER || '' : '';
      row.CRITERIO_CLUSTER = criterion ? criterion.CLUSTER || '' : '';
      row.CRITERIO_REGIONAL = criterion ? criterion.REGIONAL || '' : '';
      row.CRITERIO_TIPO_REDE = criterion ? criterion['TIPO DE REDE'] || '' : '';
      row.CRITERIO_CLUSTER_COMERCIAL = criterion ? criterion['CLUSTER COMERCIAL'] || '' : '';
      if (i && i % 5000 === 0) {
        $('#processText').textContent = `Cruzando critérios: ${i.toLocaleString('pt-BR')} de ${J.length.toLocaleString('pt-BR')} combinações...`;
        await nextPaint();
      }
    }
    groups.clear();
    selected.clear();
  }

  async function finalizeAnalyses() {
    if (streamState.finalizing || streamState.active || !okND || !okBL) return;
    streamState.finalizing = true;
    setImportLock(true);
    const button = $('#validar');
    button.disabled = true;
    showProcess('Processando análises', 'Cruzando as combinações compactas com a tabela de critérios...');
    try {
      await crossIncremental();
      updateHeadMeta(J);
      $('#processText').textContent = 'Montando filtros, rankings e gráficos...';
      await nextPaint();
      renderResult();
      fillA();
      renderA();
      await nextPaint();
      fillDailyFilters();
      await nextPaint();
      fillMonthFilters();
      ['#tAna', '#tDia', '#tMes', '#tConv'].forEach(id => $(id).disabled = false);
      setStatus('#st', 'Análises ND e BL validadas com bases compactas. Texto bruto não permanece em memória.', 'success');
      button.textContent = 'Análise Validada';
      $('#processTitle').textContent = 'Análise concluída';
      $('#processText').textContent = 'Dashboard liberado com sucesso.';
      await new Promise(resolve => setTimeout(resolve, 350));
      hideProcess();
      $('#tAna').click();
    } catch (error) {
      console.error(error);
      hideProcess();
      setStatus('#st', 'Falha na validação: ' + (error && error.message ? error.message : error), 'error');
    } finally {
      streamState.finalizing = false;
      setImportLock(false);
    }
  }

  function jsonFileName(type) {
    const meta = streamState.datasets[type] || {};
    let digits = String(meta.competence || '').replace(/\D/g, '');
    let yy = '';
    let mm = '';
    if (digits.length >= 6) {
      yy = digits.slice(2, 4);
      mm = digits.slice(4, 6);
    } else if (digits.length === 4) {
      yy = digits.slice(0, 2);
      mm = digits.slice(2, 4);
    }
    if ((!yy || !mm) && meta.maxDate) {
      const match = String(meta.maxDate).match(/(\d{4})[\/-](\d{1,2})/);
      if (match) {
        yy = match[1].slice(2, 4);
        mm = String(match[2]).padStart(2, '0');
      }
    }
    return `${type.toLowerCase()}-${yy || 'aa'}-${mm || 'mm'}.json`;
  }

  async function prepareJson(type) {
    const statusId = type === 'ND' ? '#stND' : '#stBL';
    const downloadId = type === 'ND' ? '#downloadJsonND' : '#downloadJsonBL';
    if (!streamState.datasets[type]) {
      setStatus(statusId, `Carregue e aguarde o processamento do CSV ${type}.`, 'error');
      return;
    }
    jsonDownloads[type] = {name: jsonFileName(type)};
    $(downloadId).disabled = false;
    setStatus(
      statusId,
      `JSON ${type} pronto para geração individual: ${jsonDownloads[type].name}. ` +
      'O arquivo só será serializado durante o download e não ficará duplicado em memória.',
      'success'
    );
  }

  function upsertManifestDataset(manifest, type, name, meta) {
    const output = manifest && typeof manifest === 'object' ? manifest : {};
    output.schema = 2;
    output.updatedAt = new Date().toISOString();
    output.datasets = output.datasets && typeof output.datasets === 'object' ? output.datasets : {};
    const key = type.toLowerCase();
    let entries = Array.isArray(output.datasets[key]) ? output.datasets[key].slice() : [];
    entries = entries.filter(entry => {
      const path = typeof entry === 'string' ? entry : (entry && (entry.file || entry.path || entry.name));
      return path !== name;
    });
    entries.push({
      file: name,
      format: 'json-v4',
      competence: String(meta.competence || ''),
      sourceRows: Number(meta.sourceRows) || 0,
      compactRows: Number(meta.compactRows) || 0,
      updatedAt: output.updatedAt
    });
    output.datasets[key] = entries;
    output.naming = output.naming || {nd: 'nd-aa-mm.json', bl: 'bl-aa-mm.json', competenceField: 'NR_ANO_MES'};
    return output;
  }

  async function updateSelectedDirectoryManifest(type, name, meta) {
    const directory = streamState.directoryHandle;
    if (!directory) return false;
    let manifest = null;
    try {
      const existing = await directory.getFileHandle('manifest.json');
      manifest = JSON.parse(await (await existing.getFile()).text());
    } catch (_) {
      manifest = {schema: 2, datasets: {nd: [], bl: []}};
    }
    manifest = upsertManifestDataset(manifest, type, name, meta);
    const handle = await directory.getFileHandle('manifest.json', {create: true});
    const writable = await handle.createWritable();
    await writable.write(JSON.stringify(manifest, null, 2) + '\n');
    await writable.close();
    streamState.manualManifest = manifest;
    streamState.manifestLastModified = Date.now();
    return true;
  }

  function updateManualManifest(type, name, meta) {
    streamState.manualManifest = upsertManifestDataset(streamState.manualManifest || {schema: 2, datasets: {nd: [], bl: []}}, type, name, meta);
    setImportLock(false);
  }

  function downloadManualManifest() {
    if (!streamState.manualManifest) return;
    const blob = new Blob([JSON.stringify(streamState.manualManifest, null, 2) + '\n'], {type: 'application/json;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'manifest.json';
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1500);
  }

  async function writeJson(type) {
    const statusId = type === 'ND' ? '#stND' : '#stBL';
    const button = $('#' + (type === 'ND' ? 'downloadJsonND' : 'downloadJsonBL'));
    const name = (jsonDownloads[type] && jsonDownloads[type].name) || jsonFileName(type);
    const meta = streamState.datasets[type];
    if (!meta) {
      setStatus(statusId, `Nenhuma base ${type} processada.`, 'error');
      return;
    }

    let writable = null;
    let parts = null;
    let savedInSelectedDirectory = false;
    if (streamState.directoryHandle) {
      try {
        const handle = await streamState.directoryHandle.getFileHandle(name, {create: true});
        writable = await handle.createWritable();
        savedInSelectedDirectory = true;
      } catch (error) {
        if (error && error.name === 'AbortError') return;
        setStatus(statusId, 'Não foi possível gravar na pasta selecionada; usando a alternativa manual.', 'error');
      }
    }
    if (!writable && 'showSaveFilePicker' in window && window.isSecureContext) {
      try {
        const handle = await window.showSaveFilePicker({
          suggestedName: name,
          types: [{description: 'JSON compacto', accept: {'application/json': ['.json']}}]
        });
        writable = await handle.createWritable();
      } catch (error) {
        if (error && error.name === 'AbortError') return;
        parts = [];
      }
    } else {
      parts = [];
    }

    const write = async text => {
      if (writable) await writable.write(text);
      else parts.push(text);
    };

    button.disabled = true;
    setImportLock(true);
    setStatus(statusId, `Gerando ${name} em fluxo, sem criar uma string JSON completa...`, 'processing');
    await nextPaint();

    const dictionaries = STREAM_FIELDS.map(() => []);
    const indexes = STREAM_FIELDS.map(() => new Map());
    let total = 0;
    forEachTypeRow(type, row => {
      total++;
      STREAM_FIELDS.forEach((field, index) => {
        const value = String(row[field] == null ? '' : row[field]);
        const map = indexes[index];
        if (!map.has(value)) {
          map.set(value, dictionaries[index].length);
          dictionaries[index].push(value);
        }
      });
    });

    try {
      await write('{"v":4,"t":' + JSON.stringify(type) + ',"generatedAt":' + JSON.stringify(new Date().toISOString()) +
        ',"sourceRows":' + meta.sourceRows + ',"c":' + JSON.stringify(STREAM_FIELDS) + ',"d":[');
      for (let i = 0; i < dictionaries.length; i++) {
        if (i) await write(',');
        await write('[');
        const values = dictionaries[i];
        for (let start = 0; start < values.length; start += 1000) {
          const chunk = values.slice(start, start + 1000).map(JSON.stringify).join(',');
          await write((start ? ',' : '') + chunk);
        }
        await write(']');
      }
      await write('],"r":[');

      let first = true;
      let buffer = [];
      let written = 0;
      const directRows = type === 'ND' ? P : PB;
      const sourceRows = directRows && directRows.length ? directRows : J;
      for (let sourceIndex = 0; sourceIndex < sourceRows.length; sourceIndex++) {
        const row = sourceRows[sourceIndex];
        if ((!directRows || !directRows.length) && row.ORIGEM_CONVERSAO !== type) continue;
        const packed = STREAM_FIELDS.map((field, index) => indexes[index].get(String(row[field] == null ? '' : row[field])));
        packed.push(n(row.QT));
        buffer.push(JSON.stringify(packed));
        if (buffer.length >= 1000) {
          await write((first ? '' : ',') + buffer.join(','));
          first = false;
          written += buffer.length;
          buffer.length = 0;
          if (written % 10000 === 0) {
            setStatus(statusId, `Gerando ${name}: ${written.toLocaleString('pt-BR')} de ${total.toLocaleString('pt-BR')} combinações...`, 'processing');
            await nextPaint();
          }
        }
      }
      if (buffer.length) {
        await write((first ? '' : ',') + buffer.join(','));
        written += buffer.length;
        buffer.length = 0;
      }
      await write(']}');

      if (writable) {
        await writable.close();
      } else {
        const blob = new Blob(parts, {type: 'application/json;charset=utf-8'});
        parts.length = 0;
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = name;
        anchor.click();
        setTimeout(() => URL.revokeObjectURL(url), 1500);
      }
      if (savedInSelectedDirectory) {
        await updateSelectedDirectoryManifest(type, name, meta);
        setStatus(statusId, `${name} salvo diretamente em ${streamState.directoryName}; manifest.json atualizado.`, 'success');
        setStatus('#folderStatus', `Pasta ${streamState.directoryName} atualizada com ${name} e manifest.json.`, 'success');
      } else {
        updateManualManifest(type, name, meta);
        setStatus(statusId, `${name} gerado. Use “Baixar manifest.json” e coloque ambos na pasta do OneDrive.`, 'success');
      }
    } catch (error) {
      if (writable) {
        try { await writable.abort(); } catch (_) {}
      }
      setStatus(statusId, `Falha ao gerar ${name}: ${error && error.message ? error.message : error}`, 'error');
    } finally {
      indexes.forEach(map => map.clear());
      dictionaries.length = 0;
      if (parts) parts.length = 0;
      setImportLock(false);
      button.disabled = false;
    }
  }

  function installSubclusterRanking() {
    if ($('#rankSubcluster')) return;
    const card = document.createElement('div');
    card.className = 'card subclusterCard';
    card.innerHTML = '<h2 id="rankSubclusterTitle">Ranking Subcluster x % Conversão</h2>' +
      '<p class="ranknote">Subclusters em ordem decrescente de conversão no período acumulado filtrado.</p>' +
      '<div id="rankSubcluster" class="rank rankScroll"></div>';
    document.querySelector('.rankPanels').appendChild(card);
    const style = document.createElement('style');
    style.textContent = '.rankPanels .subclusterCard{grid-column:1/-1}.workerBadge{font-weight:800;color:#1b5e20}';
    document.head.appendChild(style);
  }

  function renderSubclusterRanking() {
    const target = $('#rankSubcluster');
    if (!target || !J.length) return;
    const filters = {
      CRITERIO_UF: '#auf', CRITERIO_REGIONAL: '#areg', CRITERIO_CLUSTER: '#aclu',
      CRITERIO_CIDADE: '#ac', NM_MARCA: '#amarca', NM_LINHA_NEGOCIO: '#alinha',
      NM_PERFIL_CLIENTE: '#aperfil'
    };
    const data = analysisTypeRows().filter(row => Object.entries(filters).every(([key, id]) => hasSelected($(id), row[key])));
    const days = [...new Set(data.map(row => isoDay(row.DT)).filter(Boolean))].sort();
    const d1 = days.at(-1) || '';
    const monthData = data.filter(row => {
      const day = isoDay(row.DT);
      return day && d1 && day.slice(0, 7) === d1.slice(0, 7) && day <= d1;
    });
    const map = new Map();
    for (const row of monthData) {
      const name = row.CRITERIO_SUBCLUSTER || 'SEM SUBCLUSTER';
      const item = map.get(name) || {name, v: 0, i: 0};
      const indicator = norm(row.NM_INDICADOR_NEGOCIO);
      const quantity = n(row.QT);
      if (indicator === 'VENDA BRUTA') item.v += quantity;
      if (indicator === 'INSTALACAO') item.i += quantity;
      map.set(name, item);
    }
    const ranking = [...map.values()]
      .filter(item => norm(item.name) !== 'SEM SUBCLUSTER' && item.v > 0)
      .map(item => ({...item, p: 100 * item.i / item.v}))
      .sort((a, b) => b.p - a.p || b.i - a.i || a.name.localeCompare(b.name, 'pt-BR'));
    const max = Math.max(100, ...ranking.map(item => item.p));
    target.innerHTML = ranking.length ? ranking.map((item, index) =>
      `<div class="rankrow" title="Venda Bruta QT: ${item.v} | Instalação QT: ${item.i}">` +
      `<div class="rankname">${index + 1}. ${item.name}</div>` +
      `<div class="ranktrack"><div class="rankbar" style="width:${Math.min(100, item.p / max * 100)}%"></div></div>` +
      `<div class="rankval">${item.p.toFixed(1).replace('.', ',')}%</div></div>`
    ).join('') : '<div class="status">Sem dados para o Ranking Subcluster.</div>';
    const selectedTypes = selectedValues($('#atipo'));
    const label = selectedTypes.length === 1 ? selectedTypes[0] : (selectedTypes.length > 1 ? 'Conversão ND + BL' : 'Conversão');
    $('#rankSubclusterTitle').textContent = 'Ranking Subcluster x % Conversão - ' + label;
  }

  installDataSourcePanel();
  installSubclusterRanking();
  const originalRenderAImpl = renderAImpl;
  renderAImpl = function () {
    const result = originalRenderAImpl();
    renderSubclusterRanking();
    return result;
  };

  const ndInput = replaceFileInput('file');
  const blInput = replaceFileInput('fileBL');
  ndInput.addEventListener('change', event => processCsv('ND', event.target.files[0], ndInput));
  blInput.addEventListener('change', event => processCsv('BL', event.target.files[0], blInput));
  $('#jsonND').addEventListener('change', event => processJsonFiles('ND', event.target.files, 'JSON individual'));
  $('#jsonBL').addEventListener('change', event => processJsonFiles('BL', event.target.files, 'JSON individual'));
  $('#selectOneDriveFolder').addEventListener('click', chooseOneDriveFolder);
  $('#refreshOneDriveFolder').addEventListener('click', () => refreshSelectedDirectory(false));
  $('#oneDriveDirectoryInput').addEventListener('change', event => loadDirectoryInput(event.target));
  $('#clearND').addEventListener('click', () => clearDataset('ND'));
  $('#clearBL').addEventListener('click', () => clearDataset('BL'));
  $('#downloadManifest').addEventListener('click', downloadManualManifest);

  convertCurrentJson = prepareJson;
  downloadCurrentJson = writeJson;
  $('#convertJsonND').onclick = () => prepareJson('ND');
  $('#convertJsonBL').onclick = () => prepareJson('BL');
  $('#downloadJsonND').onclick = () => writeJson('ND');
  $('#downloadJsonBL').onclick = () => writeJson('BL');
  $('#validar').onclick = finalizeAnalyses;

  window.__streamingDashboard = {
    version: 'V24',
    state: streamState,
    fields: STREAM_FIELDS.slice(),
    processCsv,
    processJsonFiles,
    loadFolderRecords,
    refreshSelectedDirectory,
    clearDataset,
    finalizeAnalyses,
    jsonFileName
  };
  setImportLock(false);
})();
